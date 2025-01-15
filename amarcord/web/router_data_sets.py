from typing import Annotated

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import parse_schema_type
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.web.fastapi_utils import encode_data_set_attributo_value
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import json_attributo_to_data_set_orm_attributo
from amarcord.web.json_models import JsonCreateDataSetFromRun
from amarcord.web.json_models import JsonCreateDataSetFromRunOutput
from amarcord.web.json_models import JsonCreateDataSetInput
from amarcord.web.json_models import JsonCreateDataSetOutput
from amarcord.web.json_models import JsonDataSet
from amarcord.web.json_models import JsonDeleteDataSetInput
from amarcord.web.json_models import JsonDeleteDataSetOutput
from amarcord.web.json_models import JsonReadDataSets
from amarcord.web.router_attributi import encode_attributo
from amarcord.web.router_chemicals import encode_chemical
from amarcord.web.router_experiment_types import encode_experiment_type

router = APIRouter()


def _run_has_attributo_to_data_set_has_attributo(
    r: orm.RunHasAttributoValue,
) -> orm.DataSetHasAttributoValue:
    return orm.DataSetHasAttributoValue(
        attributo_id=r.attributo_id,
        integer_value=r.integer_value,
        float_value=r.float_value,
        string_value=r.string_value,
        bool_value=r.bool_value,
        datetime_value=r.datetime_value,
        list_value=r.list_value,
        chemical_value=r.chemical_value,
    )


def encode_orm_data_set_to_json(a: orm.DataSet, beamtime_id: BeamtimeId) -> JsonDataSet:
    return JsonDataSet(
        id=a.id,
        experiment_type_id=a.experiment_type_id,
        attributi=[encode_data_set_attributo_value(v) for v in a.attributo_values],
        beamtime_id=beamtime_id,
    )


@router.post(
    "/api/data-sets/from-run",
    tags=["datasets"],
    response_model_exclude_defaults=True,
)
async def create_data_set_from_run(
    input_: JsonCreateDataSetFromRun,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonCreateDataSetFromRunOutput:
    async with session.begin():
        run = (
            await session.scalars(
                select(orm.Run)
                .where(orm.Run.id == input_.run_internal_id)
                .options(selectinload(orm.Run.experiment_type)),
            )
        ).one()
        new_data_set = orm.DataSet(experiment_type_id=run.experiment_type_id)
        for et_attributo in run.experiment_type.attributi:
            for run_attributo in run.attributo_values:
                if run_attributo.attributo_id == et_attributo.attributo_id:
                    new_data_set.attributo_values.append(
                        _run_has_attributo_to_data_set_has_attributo(run_attributo),
                    )
        session.add(new_data_set)
        await session.flush()

        return JsonCreateDataSetFromRunOutput(data_set_id=new_data_set.id)


@router.post("/api/data-sets", tags=["datasets"], response_model_exclude_defaults=True)
async def create_data_set(
    input_: JsonCreateDataSetInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonCreateDataSetOutput:
    async with session.begin():
        experiment_type = (
            await session.scalars(
                select(orm.ExperimentType)
                .where(orm.ExperimentType.id == input_.experiment_type_id)
                .options(selectinload(orm.ExperimentType.data_sets)),
            )
        ).one()

        beamtime_id = experiment_type.beamtime_id
        attributi = list(
            (
                await session.scalars(
                    select(orm.Attributo).where(
                        orm.Attributo.beamtime_id == beamtime_id,
                    ),
                )
            ).all(),
        )
        attributo_types: dict[AttributoId, AttributoType] = {
            AttributoId(a.id): schema_dict_to_attributo_type(a.json_schema)
            for a in attributi
        }
        if not input_.attributi:
            raise Exception("You have to set a least one attributo value")

        new_ds_attributi = {a.attributo_id: a for a in input_.attributi}
        new_data_set = orm.DataSet(experiment_type_id=experiment_type.id)
        for eha in experiment_type.attributi:
            a = eha.attributo
            at = parse_schema_type(a.json_schema)
            attributo_value = new_ds_attributi.get(a.id)
            if attributo_value is not None:
                new_data_set.attributo_values.append(
                    json_attributo_to_data_set_orm_attributo(attributo_value),
                )
            else:
                # Boolean values can be omitted, other values can't. Booleans will be False if omitted
                if not isinstance(at, JSONSchemaBoolean):
                    raise HTTPException(
                        status_code=400,
                        detail=f"attributo {a.id} does from the experiment type does "
                        + " not appear in the data set to be created!",
                    )
                new_data_set.attributo_values.append(
                    orm.DataSetHasAttributoValue(
                        attributo_id=a.id,
                        integer_value=None,
                        float_value=None,
                        string_value=None,
                        bool_value=False,
                        datetime_value=None,
                        list_value=None,
                        chemical_value=None,
                    ),
                )
        for et_ds in experiment_type.data_sets:
            et_ds_attributi_map = {
                dsa.attributo_id: dsa for dsa in et_ds.attributo_values
            }
            new_ds_attributi_map = {
                dsa.attributo_id: dsa for dsa in new_data_set.attributo_values
            }
            if run_matches_dataset(
                attributo_types,
                et_ds_attributi_map,
                new_ds_attributi_map,
            ):
                raise HTTPException(
                    status_code=400,
                    detail=f"A data set with compatible values already exists, and its ID is {et_ds.id}",
                )

        session.add(new_data_set)
        await session.commit()

    return JsonCreateDataSetOutput(id=new_data_set.id)


@router.get(
    "/api/data-sets/{beamtimeId}",
    tags=["datasets"],
    response_model_exclude_defaults=True,
)
async def read_data_sets(
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadDataSets:
    return JsonReadDataSets(
        data_sets=[
            encode_orm_data_set_to_json(a, beamtimeId)
            for a in await session.scalars(
                select(orm.DataSet, orm.ExperimentType)
                .join(orm.DataSet.experiment_type)
                .where(orm.ExperimentType.beamtime_id == beamtimeId),
            )
        ],
        chemicals=[
            encode_chemical(a)
            for a in await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.beamtime_id == beamtimeId)
                .options(selectinload(orm.Chemical.files)),
            )
        ],
        attributi=[
            encode_attributo(a)
            for a in await session.scalars(
                select(orm.Attributo).where(
                    (orm.Attributo.associated_table == AssociatedTable.RUN)
                    & (orm.Attributo.beamtime_id == beamtimeId),
                ),
            )
        ],
        experiment_types=[
            encode_experiment_type(a)
            for a in await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.beamtime_id == beamtimeId,
                ),
            )
        ],
    )


@router.delete(
    "/api/data-sets",
    tags=["datasets"],
    response_model_exclude_defaults=True,
)
async def delete_data_set(
    input_: JsonDeleteDataSetInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonDeleteDataSetOutput:
    async with session.begin():
        await session.execute(delete(orm.DataSet).where(orm.DataSet.id == input_.id))
        await session.commit()

    return JsonDeleteDataSetOutput(result=True)
