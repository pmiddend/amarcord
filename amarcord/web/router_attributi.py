from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import convert_attributo_value
from amarcord.db.attributi import coparse_schema_type
from amarcord.db.attributi import parse_schema_type
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi import schema_union_to_attributo_type
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.ingest_attributi_from_json import ingest_run_attributi_schema
from amarcord.db.orm_utils import orm_entity_has_attributo_value_to_attributo_value
from amarcord.db.orm_utils import update_orm_entity_has_attributo_value
from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.web.constants import AUTOMATIC_ATTRIBUTI_GROUP
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonAttributo
from amarcord.web.json_models import JsonCreateAttributiFromSchemaInput
from amarcord.web.json_models import JsonCreateAttributiFromSchemaOutput
from amarcord.web.json_models import JsonCreateAttributoInput
from amarcord.web.json_models import JsonCreateAttributoOutput
from amarcord.web.json_models import JsonDeleteAttributoInput
from amarcord.web.json_models import JsonDeleteAttributoOutput
from amarcord.web.json_models import JsonReadAttributi
from amarcord.web.json_models import JsonUpdateAttributoInput
from amarcord.web.json_models import JsonUpdateAttributoOutput

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


def encode_attributo(a: orm.Attributo) -> JsonAttributo:
    schema = parse_schema_type(a.json_schema)
    # This is highly repetitive, but no way around this with the
    # current "union of types" approach
    if isinstance(schema, JSONSchemaInteger):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_integer=schema,
        )
    if isinstance(schema, JSONSchemaNumber):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_number=schema,
        )
    if isinstance(schema, JSONSchemaString):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_string=schema,
        )
    if isinstance(schema, JSONSchemaArray):
        return JsonAttributo(
            id=a.id,
            name=a.name,
            description=a.description,
            group=a.group,
            associated_table=a.associated_table,
            attributo_type_array=schema,
        )
    return JsonAttributo(
        id=a.id,
        name=a.name,
        description=a.description,
        group=a.group,
        associated_table=a.associated_table,
        attributo_type_boolean=schema,
    )


@router.post(
    "/api/attributi/schema",
    tags=["attributi"],
    response_model_exclude_defaults=True,
)
async def create_attributi_from_schema(
    input_: JsonCreateAttributiFromSchemaInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonCreateAttributiFromSchemaOutput:
    logger.info("ingesting attributi schema")

    async with session.begin():
        attributi_schema = input_.attributi_schema
        beamtime_id = input_.beamtime_id
        ingest_run_attributi_schema(
            session,
            beamtime_id,
            preexisting_attributi_by_name={
                a.name: schema_union_to_attributo_type(parse_schema_type(a.json_schema))
                for a in await session.scalars(
                    select(orm.Attributo).where(
                        orm.Attributo.beamtime_id == input_.beamtime_id,
                    ),
                )
            },
            attributi_schema={
                s.attributo_name: (s.description, s.attributo_type)
                for s in attributi_schema
            },
            group=AUTOMATIC_ATTRIBUTI_GROUP,
        )
        await session.commit()
        return JsonCreateAttributiFromSchemaOutput(
            created_attributi=len(input_.attributi_schema),
        )


@router.post("/api/attributi", tags=["attributi"], response_model_exclude_defaults=True)
async def create_attributo(
    input_: JsonCreateAttributoInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonCreateAttributoOutput:
    async with session.begin():
        new_attributo = orm.Attributo(
            beamtime_id=input_.beamtime_id,
            name=input_.name,
            description=input_.description,
            group=input_.group,
            associated_table=input_.associated_table,
            # wild: convert schema to attributo type (from external
            # API schema representation to internal representation);
            # then convert that to the internal schema representation
            # (in the DB)
            json_schema=coparse_schema_type(
                attributo_type_to_schema(
                    schema_to_attributo_type(
                        schema_number=input_.attributo_type_number,
                        schema_boolean=input_.attributo_type_boolean,
                        schema_integer=input_.attributo_type_integer,
                        schema_array=input_.attributo_type_array,
                        schema_string=input_.attributo_type_string,
                    ),
                ),
            ),
        )
        session.add(new_attributo)
        await session.commit()

        return JsonCreateAttributoOutput(id=new_attributo.id)


async def _update_geometries(
    session: AsyncSession,
    attributo_id: int,
    attributo_old_name: str,
    attributo_new_name: str,
) -> None:
    exists_subquery = (
        select(orm.geometry_references_attributo.c.geometry_id)
        .where(
            (orm.geometry_references_attributo.c.geometry_id == orm.Geometry.id)
            & (orm.geometry_references_attributo.c.attributo_id == attributo_id)
        )
        .exists()
    )
    for g in await session.scalars(select(orm.Geometry).where(exists_subquery)):
        needle = "{{" + attributo_old_name + "}}"
        if needle not in g.content:
            raise Exception(
                f"something is off; I can't find the needle {needle} in geometry {g.id}"
            )
        g.content = g.content.replace(needle, "{{" + attributo_new_name + "}}")


@router.patch(
    "/api/attributi",
    tags=["attributi"],
    response_model_exclude_defaults=True,
)
async def update_attributo(
    input_: JsonUpdateAttributoInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonUpdateAttributoOutput:
    async with session.begin():
        attributo = (
            await session.scalars(
                select(orm.Attributo).where(orm.Attributo.id == input_.attributo.id),
            )
        ).one()
        original_name = attributo.name
        attributo.name = input_.attributo.name
        attributo.description = input_.attributo.description
        attributo.group = input_.attributo.group
        attributo.associated_table = input_.attributo.associated_table
        before_type = schema_union_to_attributo_type(
            parse_schema_type(attributo.json_schema),
        )
        after_type = schema_to_attributo_type(
            schema_number=input_.attributo.attributo_type_number,
            schema_boolean=input_.attributo.attributo_type_boolean,
            schema_integer=input_.attributo.attributo_type_integer,
            schema_array=input_.attributo.attributo_type_array,
            schema_string=input_.attributo.attributo_type_string,
        )
        attributo.json_schema = coparse_schema_type(
            attributo_type_to_schema(after_type),
        )

        conversion_flags = AttributoConversionFlags(
            ignore_units=input_.conversion_flags.ignore_units,
        )
        entities: list[
            orm.ChemicalHasAttributoValue
            | orm.DataSetHasAttributoValue
            | orm.RunHasAttributoValue
        ] = []
        entities.extend(
            await session.scalars(
                select(orm.ChemicalHasAttributoValue).where(
                    orm.ChemicalHasAttributoValue.attributo_id == attributo.id,
                ),
            ),
        )
        entities.extend(
            await session.scalars(
                select(orm.RunHasAttributoValue).where(
                    orm.RunHasAttributoValue.attributo_id == attributo.id,
                ),
            ),
        )
        entities.extend(
            await session.scalars(
                select(orm.DataSetHasAttributoValue).where(
                    orm.DataSetHasAttributoValue.attributo_id == attributo.id,
                ),
            ),
        )
        for chemical_value in entities:
            update_orm_entity_has_attributo_value(
                chemical_value,
                after_type,
                convert_attributo_value(
                    before_type,
                    after_type,
                    conversion_flags,
                    orm_entity_has_attributo_value_to_attributo_value(chemical_value),
                ),
            )

        if original_name != input_.attributo.name:
            await _update_geometries(
                session, input_.attributo.id, original_name, input_.attributo.name
            )

    return JsonUpdateAttributoOutput(id=input_.attributo.id)


@router.delete(
    "/api/attributi",
    tags=["attributi"],
    response_model_exclude_defaults=True,
)
async def delete_attributo(
    input_: JsonDeleteAttributoInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonDeleteAttributoOutput:
    async with session.begin():
        exists_subquery = (
            select(orm.geometry_references_attributo.c.geometry_id)
            .where(
                (orm.geometry_references_attributo.c.geometry_id == orm.Geometry.id)
                & (orm.geometry_references_attributo.c.attributo_id == input_.id)
            )
            .exists()
        )
        geometry_names_with_attributo: list[str] = [
            g.name
            for g in await session.scalars(select(orm.Geometry).where(exists_subquery))
        ]

        if geometry_names_with_attributo:
            raise HTTPException(
                status_code=400,
                detail="The following geometries still use this attribute,  you cannot delete it: "
                + ", ".join(geometry_names_with_attributo),
            )

        await session.execute(
            delete(orm.Attributo).where(orm.Attributo.id == input_.id),
        )
        await session.commit()
        return JsonDeleteAttributoOutput(id=input_.id)


@router.get(
    "/api/attributi/{beamtimeId}",
    tags=["attributi"],
    response_model_exclude_defaults=True,
)
async def read_attributi(
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadAttributi:
    return JsonReadAttributi(
        attributi=[
            encode_attributo(a)
            for a in await session.scalars(
                select(orm.Attributo).where(orm.Attributo.beamtime_id == beamtimeId),
            )
        ],
    )
