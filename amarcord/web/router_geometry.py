import datetime
from typing import Annotated
from typing import Generator

import mstache
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Response
from fastapi.responses import PlainTextResponse
from sqlalchemy import delete
from sqlalchemy import func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.cli.crystfel_index import sha256_bytes
from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.geometry_type import GeometryType
from amarcord.db.orm_utils import encode_beamtime
from amarcord.db.orm_utils import render_template
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonGeometryCopyToBeamtime
from amarcord.web.json_models import JsonGeometryCreate
from amarcord.web.json_models import JsonGeometryUpdate
from amarcord.web.json_models import JsonGeometryWithoutContent
from amarcord.web.json_models import JsonGeometryWithUsages
from amarcord.web.json_models import JsonReadGeometriesForAllBeamtimes
from amarcord.web.json_models import JsonReadGeometriesForSingleBeamtime
from amarcord.web.json_models import JsonReadSingleGeometryOutput
from amarcord.web.router_attributi import encode_attributo

router = APIRouter()


def _template_variable_names(content: str) -> Generator[str, None, None]:
    return (
        scope_key.decode("utf-8")
        for _, _, _, scope_key, _, _ in mstache.tokenize(content.encode("utf-8"))
        if len(scope_key)
    )


async def _check_variable_names_are_valid_and_return_attributi(
    session: AsyncSession, beamtime_id: BeamtimeId, content: str
) -> list[orm.Attributo]:
    invalid_names: list[str] = []
    attributi: list[orm.Attributo] = []
    attributo_name_to_attributo: dict[str, orm.Attributo] = {
        a.name: a
        for a in await session.scalars(
            select(orm.Attributo).where(orm.Attributo.beamtime_id == beamtime_id)
        )
    }

    for name in _template_variable_names(content):
        attributo = attributo_name_to_attributo.get(name)
        if attributo is None:
            invalid_names.append(name)
        else:
            attributi.append(attributo)

    if invalid_names:
        raise HTTPException(
            status_code=400,
            detail="the following template variables don't correspond to Attributo names: "
            + ", ".join(invalid_names),
        )

    return attributi


@router.post("/api/geometries", tags=["geometries"])
async def create_geometry(
    input_: JsonGeometryCreate,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonGeometryWithoutContent:
    async with session.begin():
        hash_ = sha256_bytes(input_.content.encode("utf-8"))
        created = datetime.datetime.now(datetime.timezone.utc)

        attributi = await _check_variable_names_are_valid_and_return_attributi(
            session, input_.beamtime_id, input_.content
        )

        geometry_type = (
            GeometryType.CRYSTFEL_FILE
            if input_.content.startswith("/")
            else GeometryType.CRYSTFEL_STRING
        )
        new_geometry = orm.Geometry(
            beamtime_id=input_.beamtime_id,
            content=input_.content,
            name=input_.name,
            hash=hash_,
            created=created,
            geometry_type=geometry_type,
        )
        for a in attributi:
            new_geometry.attributi.append(a)
        existing_geometry = (
            await session.scalars(
                select(orm.Geometry).where(
                    (orm.Geometry.name == input_.name)
                    & (orm.Geometry.beamtime_id == input_.beamtime_id)
                ),
            )
        ).one_or_none()
        if existing_geometry:
            raise HTTPException(
                status_code=400,
                detail=f"geometry with name {existing_geometry} already exists",
            )
        session.add(new_geometry)
        # we need to the ID in the next line, so have to flush
        await session.flush()
        return JsonGeometryWithoutContent(
            id=new_geometry.id,
            beamtime_id=input_.beamtime_id,
            name=input_.name,
            hash=hash_,
            created=utc_datetime_to_utc_int(created),
            created_local=utc_datetime_to_local_int(created),
            attributi=[a.id for a in attributi],
            geometry_type=geometry_type,
        )


@router.post("/api/geometry-copy-to-beamtime", tags=["geometries"])
async def copy_to_beamtime(
    input_: JsonGeometryCopyToBeamtime,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonGeometryWithoutContent:
    async with session.begin():
        geometry_to_copy = (
            await session.scalars(
                select(orm.Geometry).where(orm.Geometry.id == input_.geometry_id),
            )
        ).one_or_none()
        if geometry_to_copy is None:
            raise HTTPException(
                status_code=404,
                detail=f"geometry with id {input_.geometry_id} not found",
            )
        existing_geometry = (
            await session.scalars(
                select(orm.Geometry).where(
                    (orm.Geometry.name == geometry_to_copy.name)
                    & (orm.Geometry.beamtime_id == input_.target_beamtime_id)
                ),
            )
        ).one_or_none()
        if existing_geometry is not None:
            raise HTTPException(
                status_code=400,
                detail=f"geometry with name {geometry_to_copy.name} already exists",
            )
        created = datetime.datetime.now(datetime.timezone.utc)
        attributi = await _check_variable_names_are_valid_and_return_attributi(
            session, input_.target_beamtime_id, geometry_to_copy.content
        )
        new_geometry = orm.Geometry(
            beamtime_id=input_.target_beamtime_id,
            content=geometry_to_copy.content,
            name=geometry_to_copy.name,
            hash=geometry_to_copy.hash,
            created=created,
            geometry_type=geometry_to_copy.geometry_type,
        )
        for a in attributi:
            new_geometry.attributi.append(a)
        session.add(new_geometry)
        # we need to the ID in the next line, so have to flush
        await session.flush()
        return JsonGeometryWithoutContent(
            id=new_geometry.id,
            beamtime_id=input_.target_beamtime_id,
            name=geometry_to_copy.name,
            hash=geometry_to_copy.hash,
            created=utc_datetime_to_utc_int(created),
            created_local=utc_datetime_to_local_int(created),
            attributi=[],
            geometry_type=geometry_to_copy.geometry_type,
        )


async def _count_usages(session: AsyncSession, geometry_ids: list[int]) -> int:
    # This doesn't scale with the amount of geometries. If that
    # becomes an issue, we can just use a nested select statement,
    # problem solved.
    parameter_count = await session.scalar(
        select(func.count(orm.IndexingParameters.id)).where(
            orm.IndexingParameters.geometry_id.in_(geometry_ids)
        )
    )
    # For some reason, scalar returns "None | int" here, but how can
    # it be None? Even with zero rows, it'll return "0"
    assert parameter_count is not None
    result_count = await session.scalar(
        select(func.count(orm.IndexingResult.id)).where(
            orm.IndexingResult.generated_geometry_id.in_(geometry_ids)
        )
    )
    # See above
    assert result_count is not None
    return parameter_count + result_count


@router.patch("/api/geometries/{geometryId}", tags=["geometries"])
async def update_geometry(
    geometryId: int,  # noqa: N803
    input_: JsonGeometryUpdate,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonGeometryWithoutContent:
    async with session.begin():
        existing_geometry = (
            await session.scalars(
                select(orm.Geometry)
                .where(orm.Geometry.id == geometryId)
                .options(selectinload(orm.Geometry.attributi)),
            )
        ).one_or_none()
        if existing_geometry is None:
            raise HTTPException(
                status_code=404, detail=f"geometry with id {geometryId} not found"
            )
        existing_geometry_with_name = (
            await session.scalars(
                select(orm.Geometry).where(
                    (orm.Geometry.name == input_.name)
                    & (orm.Geometry.id != geometryId)
                    & (orm.Geometry.beamtime_id == existing_geometry.beamtime_id)
                ),
            )
        ).one_or_none()
        if existing_geometry_with_name is not None:
            raise HTTPException(
                status_code=404,
                detail=f"geometry with name {input_.name} already in beamtime",
            )
        if existing_geometry.content != input_.content:
            usages = await _count_usages(session, [geometryId])
            if usages > 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"geometry {geometryId} is used {usages} times and the content cannot be updated",
                )

        attributi = await _check_variable_names_are_valid_and_return_attributi(
            session, existing_geometry.beamtime_id, input_.content
        )
        existing_geometry.attributi.clear()
        await session.flush()
        for a in attributi:
            existing_geometry.attributi.append(a)
        hash_ = sha256_bytes(input_.content.encode("utf-8"))
        existing_geometry.content = input_.content
        existing_geometry.hash = hash_
        existing_geometry.name = input_.name
        existing_geometry.geometry_type = (
            GeometryType.CRYSTFEL_FILE
            if input_.content.startswith("/")
            else GeometryType.CRYSTFEL_STRING
        )
        session.add(existing_geometry)
        await session.flush()
        return JsonGeometryWithoutContent(
            id=geometryId,
            beamtime_id=existing_geometry.beamtime_id,
            name=input_.name,
            hash=hash_,
            created=utc_datetime_to_utc_int(existing_geometry.created),
            created_local=utc_datetime_to_local_int(existing_geometry.created),
            attributi=[a.id for a in attributi],
            geometry_type=existing_geometry.geometry_type,
        )


async def _retrieve_single_beamtime_geometries(
    session: AsyncSession, beamtime_id: BeamtimeId
) -> JsonReadGeometriesForSingleBeamtime:
    geometries = [
        JsonGeometryWithoutContent(
            id=geom.id,
            beamtime_id=geom.beamtime_id,
            hash=geom.hash,
            name=geom.name,
            created=utc_datetime_to_utc_int(geom.created),
            created_local=utc_datetime_to_local_int(geom.created),
            attributi=[a.id for a in geom.attributi],
            geometry_type=geom.geometry_type,
        )
        for geom in await session.scalars(
            select(orm.Geometry)
            .where(orm.Geometry.beamtime_id == beamtime_id)
            .options(selectinload(orm.Geometry.attributi))
        )
    ]
    # For every geometry ID, store the number of usages
    result: dict[int, int] = {}
    # Important: use execute here since we're selecting individual columns
    for row in await session.execute(
        select(orm.Geometry.id, func.count().label("count"))
        .join(
            orm.IndexingParameters,
            orm.IndexingParameters.geometry_id == orm.Geometry.id,
        )
        .group_by(orm.Geometry.id)
    ):
        result[row.id] = row.count  # type: ignore
    # Important: use execute here since we're selecting individual columns
    for row in await session.execute(
        select(orm.Geometry.id, func.count().label("count"))
        .join(
            orm.IndexingResult,
            orm.IndexingResult.generated_geometry_id == orm.Geometry.id,
        )
        .group_by(orm.Geometry.id)
    ):
        if row.id not in result:
            result[row.id] = 0
        result[row.id] += row.count  # type: ignore

    return JsonReadGeometriesForSingleBeamtime(
        geometries=geometries,
        geometry_with_usage=[
            JsonGeometryWithUsages(geometry_id=geometry_id, usages=geometry_usage_count)
            for geometry_id, geometry_usage_count in result.items()
        ],
        attributi=[
            encode_attributo(a)
            for a in await session.scalars(
                select(orm.Attributo).where(orm.Attributo.beamtime_id == beamtime_id)
            )
        ],
    )


@router.delete(
    "/api/geometries/{geometryId}",
    tags=["geometries"],
    response_model_exclude_defaults=True,
)
async def delete_single_geometry(
    geometryId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadGeometriesForSingleBeamtime:
    async with session.begin():
        beamtime_id = (
            await session.scalars(
                select(orm.Geometry.beamtime_id).where(orm.Geometry.id == geometryId)
            )
        ).one()
        await session.execute(delete(orm.Geometry).where(orm.Geometry.id == geometryId))
        return await _retrieve_single_beamtime_geometries(session, beamtime_id)


@router.get(
    "/api/geometry-for-beamtime/{beamtimeId}",
    tags=["geometries"],
    response_model_exclude_defaults=True,
)
async def read_geometries_for_single_beamtime(
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadGeometriesForSingleBeamtime:
    return await _retrieve_single_beamtime_geometries(session, beamtimeId)


@router.get(
    "/api/all-geometries",
    tags=["geometries"],
    response_model_exclude_defaults=True,
)
async def read_geometries_for_all_beamtimes(
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadGeometriesForAllBeamtimes:
    result = list(
        (
            await session.scalars(
                select(orm.Geometry)
                .options(selectinload(orm.Geometry.beamtime))
                .options(selectinload(orm.Geometry.attributi))
            )
        ).fetchall()
    )
    return JsonReadGeometriesForAllBeamtimes(
        beamtimes=[
            encode_beamtime(geom.beamtime, with_chemicals=False) for geom in result
        ],
        geometries=[
            JsonGeometryWithoutContent(
                id=geom.id,
                beamtime_id=geom.beamtime_id,
                hash=geom.hash,
                name=geom.name,
                created=utc_datetime_to_utc_int(geom.created),
                created_local=utc_datetime_to_local_int(geom.created),
                attributi=[a.id for a in geom.attributi],
                geometry_type=geom.geometry_type,
            )
            for geom in result
        ],
    )


@router.get(
    "/api/geometries/{geometryId}/raw",
    tags=["geometries"],
    response_model_exclude_defaults=True,
    response_class=PlainTextResponse,
)
async def read_single_geometry_raw(
    geometryId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
    indexingResultId: None | int = None,  # noqa: N803
) -> Response:
    geometry = (
        await session.scalars(select(orm.Geometry).where(orm.Geometry.id == geometryId))
    ).one()
    content = geometry.content
    if indexingResultId is not None:
        indexing_result = (
            await session.scalars(
                select(orm.IndexingResult)
                .where(orm.IndexingResult.id == indexingResultId)
                .options(
                    selectinload(orm.IndexingResult.template_replacements).selectinload(
                        orm.GeometryTemplateReplacement.attributo
                    )
                )
            )
        ).one()

        content = render_template(content, indexing_result.template_replacements)

    return Response(
        content=content,
        media_type="text/plain",
    )


@router.get(
    "/api/geometries/{geometryId}",
    tags=["geometries"],
    response_model_exclude_defaults=True,
)
async def read_single_geometry(
    geometryId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadSingleGeometryOutput:
    geom = (
        await session.scalars(
            select(orm.Geometry)
            .where(orm.Geometry.id == geometryId)
            .options(selectinload(orm.Geometry.attributi))
        )
    ).one()
    return JsonReadSingleGeometryOutput(
        content=geom.content, attributi=[a.id for a in geom.attributi]
    )
