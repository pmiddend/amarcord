import datetime

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.orm_utils import validate_json_attributo_return_error
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import json_attributo_to_chemical_orm_attributo
from amarcord.web.fastapi_utils import update_attributi_from_json
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonChemical
from amarcord.web.json_models import JsonChemicalWithId
from amarcord.web.json_models import JsonChemicalWithoutId
from amarcord.web.json_models import JsonCreateChemicalOutput
from amarcord.web.json_models import JsonDeleteChemicalInput
from amarcord.web.json_models import JsonDeleteChemicalOutput
from amarcord.web.json_models import JsonReadChemicals
from amarcord.web.router_attributi import encode_attributo
from amarcord.web.router_files import encode_file_output

router = APIRouter()


@router.post("/api/chemicals", tags=["chemicals"])
async def create_chemical(
    input_: JsonChemicalWithoutId, session: AsyncSession = Depends(get_orm_db)
) -> JsonCreateChemicalOutput:
    async with session.begin():
        beamtime_id = input_.beamtime_id

        new_chemical = orm.Chemical(
            beamtime_id=beamtime_id,
            name=input_.name,
            responsible_person=input_.responsible_person,
            type=input_.chemical_type,
            modified=datetime.datetime.now(datetime.timezone.utc),
        )

        attributi_by_id: dict[int, orm.Attributo] = {
            a.id: a
            for a in (
                await session.scalars(
                    select(orm.Attributo).where(
                        orm.Attributo.id.in_(a.attributo_id for a in input_.attributi)
                        & (orm.Attributo.associated_table == AssociatedTable.CHEMICAL)
                    )
                )
            )
        }

        for a in input_.attributi:
            attributo_type = attributi_by_id.get(a.attributo_id)
            if attributo_type is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"attributo with ID {a.attributo_id} not found in list of chemical attributi",
                )
            validation_result = validate_json_attributo_return_error(a, attributo_type)
            if validation_result is not None:
                raise HTTPException(
                    status_code=400,
                    detail=f"error validating attributi: {validation_result}",
                )
            new_chemical.attributo_values.append(
                json_attributo_to_chemical_orm_attributo(a)
            )

        new_chemical.files.extend(
            await session.scalars(
                select(orm.File).where(orm.File.id.in_(input_.file_ids))
            )
        )
        session.add(new_chemical)
        await session.commit()

    return JsonCreateChemicalOutput(id=new_chemical.id)


@router.patch("/api/chemicals", tags=["chemicals"])
async def update_chemical(
    input_: JsonChemicalWithId, session: AsyncSession = Depends(get_orm_db)
) -> JsonCreateChemicalOutput:
    async with session.begin():
        existing_chemical = (
            await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.id == input_.id)
                .options(selectinload(orm.Chemical.files))
            )
        ).one()

        await update_attributi_from_json(
            session,
            db_item=existing_chemical,
            new_attributi=input_.attributi,
            attributi_by_id={
                a.id: a
                for a in (
                    await session.scalars(
                        select(orm.Attributo).where(
                            orm.Attributo.id.in_(
                                a.attributo_id for a in input_.attributi
                            )
                            & (
                                orm.Attributo.associated_table
                                == AssociatedTable.CHEMICAL
                            )
                        )
                    )
                )
            },
        )
        existing_chemical.responsible_person = input_.responsible_person
        existing_chemical.name = input_.name
        existing_chemical.type = input_.chemical_type

        existing_chemical.files.clear()

        existing_chemical.files.extend(
            await session.scalars(
                select(orm.File).where(orm.File.id.in_(input_.file_ids))
            )
        )
        await session.commit()

    return JsonCreateChemicalOutput(id=input_.id)


@router.delete("/api/chemicals", tags=["chemicals"])
async def delete_chemical(
    input_: JsonDeleteChemicalInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonDeleteChemicalOutput:
    async with session.begin():
        await session.execute(delete(orm.Chemical).where(orm.Chemical.id == input_.id))
        await session.commit()

    return JsonDeleteChemicalOutput(id=input_.id)


def _encode_chemical_attributo_value(
    d: orm.ChemicalHasAttributoValue,
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=d.attributo_id,
        attributo_value_str=d.string_value,
        attributo_value_int=d.integer_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(d.datetime_value)
            if d.datetime_value is not None
            else None
        ),
        attributo_value_float=d.float_value,
        attributo_value_bool=d.bool_value,
        attributo_value_list_str=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], str)  # type: ignore
            else None
        ),
        attributo_value_list_float=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], float)  # type: ignore
            else None
        ),
        attributo_value_list_bool=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], bool)  # type: ignore
            else None
        ),
    )


def encode_chemical(a: orm.Chemical) -> JsonChemical:
    return JsonChemical(
        id=a.id,
        beamtime_id=a.beamtime_id,
        name=a.name,
        responsible_person=a.responsible_person,
        chemical_type=a.type,
        attributi=[_encode_chemical_attributo_value(v) for v in a.attributo_values],
        files=[encode_file_output(f) for f in a.files],
    )


@router.get(
    "/api/chemicals/{beamtimeId}",
    tags=["chemicals"],
    response_model_exclude_defaults=True,
)
async def read_chemicals(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
) -> JsonReadChemicals:
    return JsonReadChemicals(
        chemicals=[
            encode_chemical(a)
            for a in await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.beamtime_id == beamtimeId)
                .options(selectinload(orm.Chemical.files))
            )
        ],
        attributi=[
            encode_attributo(a)
            for a in await session.scalars(
                select(orm.Attributo).where(
                    (orm.Attributo.associated_table == AssociatedTable.CHEMICAL)
                    & (orm.Attributo.beamtime_id == beamtimeId)
                )
            )
        ],
    )
