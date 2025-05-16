from typing import Annotated
import datetime

from fastapi import APIRouter, HTTPException, Response
from fastapi import Depends
from fastapi.responses import PlainTextResponse
from sqlalchemy import delete, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.cli.crystfel_index import sha256_bytes
from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int, utc_datetime_to_utc_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.db.orm_utils import encode_beamtime
from amarcord.web.json_models import (
    JsonGeometryCopyToBeamtime,
    JsonGeometryCreate,
    JsonGeometryWithUsages,
    JsonGeometryWithoutContent,
    JsonGeometryUpdate,
    JsonReadGeometriesForAllBeamtimes,
    JsonReadGeometriesForSingleBeamtime,
    JsonReadSingleGeometryOutput,
)

router = APIRouter()


@router.post("/api/geometries", tags=["geometries"])
async def create_geometry(
    input_: JsonGeometryCreate,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonGeometryWithoutContent:
    async with session.begin():
        hash = sha256_bytes(input_.content.encode("utf-8"))
        created = datetime.datetime.now(datetime.timezone.utc)
        new_geometry = orm.Geometry(
            beamtime_id=input_.beamtime_id,
            content=input_.content,
            name=input_.name,
            hash=hash,
            created=created,
        )
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
                detail=f"gemoetry with name {existing_geometry} already exists",
            )
        session.add(new_geometry)
        # we need to the ID in the next line, so have to flush
        await session.flush()
        return JsonGeometryWithoutContent(
            id=new_geometry.id,
            beamtime_id=input_.beamtime_id,
            name=input_.name,
            hash=hash,
            created=utc_datetime_to_utc_int(created),
            created_local=utc_datetime_to_local_int(created),
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
        new_geometry = orm.Geometry(
            beamtime_id=input_.target_beamtime_id,
            content=geometry_to_copy.content,
            name=geometry_to_copy.name,
            hash=geometry_to_copy.hash,
            created=created,
        )
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
        )


async def _count_usages(session: AsyncSession, geometry_ids: list[int]) -> int:
    # This doesn't scale with the amount of geometries. If that
    # becomes an issue, we can just use a nested select statement,
    # problem solved.
    parameter_count = await session.scalar(
        select(func.count(orm.IndexingParameters.id)).where(
            (orm.IndexingParameters.geometry_id.in_(geometry_ids))
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
    geometryId: int,
    input_: JsonGeometryUpdate,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonGeometryWithoutContent:
    async with session.begin():
        existing_geometry = (
            await session.scalars(
                select(orm.Geometry).where(orm.Geometry.id == geometryId),
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

        hash = sha256_bytes(input_.content.encode("utf-8"))
        existing_geometry.content = input_.content
        existing_geometry.hash = hash
        existing_geometry.name = input_.name
        session.add(existing_geometry)
        await session.flush()
        return JsonGeometryWithoutContent(
            id=geometryId,
            beamtime_id=existing_geometry.beamtime_id,
            name=input_.name,
            hash=hash,
            created=utc_datetime_to_utc_int(existing_geometry.created),
            created_local=utc_datetime_to_local_int(existing_geometry.created),
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
        )
        for geom in await session.scalars(
            select(orm.Geometry).where(orm.Geometry.beamtime_id == beamtime_id)
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
        result[row.id] = row.count
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
        result[row.id] += row.count

    return JsonReadGeometriesForSingleBeamtime(
        geometries=geometries,
        geometry_with_usage=[
            JsonGeometryWithUsages(geometry_id=geometry_id, usages=geometry_usage_count)
            for geometry_id, geometry_usage_count in result.items()
        ],
    )


@router.delete(
    "/api/geometries/{geometryId}",
    tags=["geometries"],
    response_model_exclude_defaults=True,
)
async def delete_single_geometry(
    geometryId: int,
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
    beamtimeId: BeamtimeId,
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
                select(orm.Geometry).options(selectinload(orm.Geometry.beamtime))
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
) -> Response:
    return Response(
        content=(
            await session.scalars(
                select(orm.Geometry).where(orm.Geometry.id == geometryId)
            )
        )
        .one()
        .content,
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
    return JsonReadSingleGeometryOutput(
        content=(
            await session.scalars(
                select(orm.Geometry).where(orm.Geometry.id == geometryId)
            )
        )
        .one()
        .content
    )
