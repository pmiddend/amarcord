from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.orm_utils import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.orm_utils import create_new_user_configuration
from amarcord.db.orm_utils import retrieve_latest_config
from amarcord.web.fastapi_utils import format_run_id_intervals
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonAttributiIdAndRole
from amarcord.web.json_models import JsonChangeRunExperimentType
from amarcord.web.json_models import JsonChangeRunExperimentTypeOutput
from amarcord.web.json_models import JsonCreateExperimentTypeInput
from amarcord.web.json_models import JsonCreateExperimentTypeOutput
from amarcord.web.json_models import JsonDeleteExperimentType
from amarcord.web.json_models import JsonDeleteExperimentTypeOutput
from amarcord.web.json_models import JsonExperimentType
from amarcord.web.json_models import JsonExperimentTypeAndRuns
from amarcord.web.json_models import JsonReadExperimentTypes
from amarcord.web.router_attributi import encode_attributo

router = APIRouter()


def encode_experiment_type(a: orm.ExperimentType) -> JsonExperimentType:
    return JsonExperimentType(
        id=a.id,
        name=a.name,
        attributi=[
            JsonAttributiIdAndRole(id=ea.attributo_id, role=ea.chemical_role)
            for ea in a.attributi
        ],
    )


@router.post(
    "/api/experiment-types",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def create_experiment_type(
    input_: JsonCreateExperimentTypeInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonCreateExperimentTypeOutput:
    async with session.begin():
        new_experiment_type = orm.ExperimentType(
            beamtime_id=input_.beamtime_id, name=input_.name
        )
        for a in input_.attributi:
            new_experiment_type.attributi.append(
                orm.ExperimentHasAttributo(attributo_id=a.id, chemical_role=a.role)
            )
        session.add(new_experiment_type)
        await session.commit()
        return JsonCreateExperimentTypeOutput(id=new_experiment_type.id)


@router.post(
    "/api/experiment-types/change-for-run",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def change_current_run_experiment_type(
    input_: JsonChangeRunExperimentType, session: AsyncSession = Depends(get_orm_db)
) -> JsonChangeRunExperimentTypeOutput:
    # There is no semantic yet for resetting an experiment type
    if input_.experiment_type_id is None:
        return JsonChangeRunExperimentTypeOutput(result=False)

    async with session.begin():
        experiment_type = (
            await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.id == input_.experiment_type_id
                )
            )
        ).one()

        experiment_type_attributo_ids = set(
            x.attributo_id for x in experiment_type.attributi
        )

        run = (
            await session.scalars(
                select(orm.Run)
                .where(orm.Run.id == input_.run_internal_id)
                .options(
                    selectinload(orm.Run.attributo_values).selectinload(
                        orm.RunHasAttributoValue.attributo
                    )
                )
            )
        ).one()

        for av in [
            av
            for av in run.attributo_values
            if av.attributo.group == ATTRIBUTO_GROUP_MANUAL
            and av.attributo_id not in experiment_type_attributo_ids
        ]:
            await session.delete(av)

        run.experiment_type_id = input_.experiment_type_id

        new_config = create_new_user_configuration(
            await retrieve_latest_config(session, run.beamtime_id)
        )
        new_config.current_experiment_type_id = input_.experiment_type_id
        session.add(new_config)
        await session.commit()
        return JsonChangeRunExperimentTypeOutput(result=True)


@router.get(
    "/api/experiment-types/{beamtimeId}",
    tags=["experimenttypes"],
    response_model_exclude_defaults=True,
)
async def read_experiment_types(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
) -> JsonReadExperimentTypes:
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )
    experiment_types = (
        await session.scalars(
            select(orm.ExperimentType)
            .where(orm.ExperimentType.beamtime_id == beamtimeId)
            .options(selectinload(orm.ExperimentType.attributi))
            .options(selectinload(orm.ExperimentType.runs))
        )
    ).all()
    return JsonReadExperimentTypes(
        experiment_types=[encode_experiment_type(a) for a in experiment_types],
        attributi=[encode_attributo(a) for a in attributi],
        experiment_type_id_to_run=[
            JsonExperimentTypeAndRuns(
                id=et.id,
                runs=format_run_id_intervals(r.external_id for r in et.runs),
            )
            for et in experiment_types
        ],
    )


@router.delete("/api/experiment-types", tags=["experimenttypes"])
async def delete_experiment_type(
    input_: JsonDeleteExperimentType, session: AsyncSession = Depends(get_orm_db)
) -> JsonDeleteExperimentTypeOutput:
    async with session.begin():
        await session.execute(
            delete(orm.ExperimentType).where(orm.ExperimentType.id == input_.id)
        )
        await session.commit()

    return JsonDeleteExperimentTypeOutput(result=True)
