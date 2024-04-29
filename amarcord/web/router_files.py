import hashlib
import os
import uuid
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Annotated
from typing import Any

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import UploadFile
from fastapi.params import Form
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.orm_utils import create_file_in_db
from amarcord.db.orm_utils import duplicate_file
from amarcord.db.orm_utils import live_stream_image_name
from amarcord.db.orm_utils import update_file_with_contents
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonCreateFileOutput
from amarcord.web.json_models import JsonCreateLiveStreamSnapshotOutput
from amarcord.web.json_models import JsonDeleteFileInput
from amarcord.web.json_models import JsonDeleteFileOutput
from amarcord.web.json_models import JsonFileOutput
from amarcord.web.json_models import JsonUpdateLiveStream

router = APIRouter()
logger = structlog.stdlib.get_logger(__name__)


def encode_file_output(f: orm.File) -> JsonFileOutput:
    assert f.id is not None
    return JsonFileOutput(
        id=f.id,
        description=f.description,
        type_=f.type,
        file_name=f.file_name,
        size_in_bytes=f.size_in_bytes,
        original_path=f.original_path,
    )


@router.post("/api/files", tags=["files"], response_model_exclude_defaults=True)
async def create_file(
    file: UploadFile,
    description: Annotated[str, Form()],
    deduplicate: Annotated[str, Form()],
    session: AsyncSession = Depends(get_orm_db),
) -> JsonCreateFileOutput:
    async with session.begin():
        file_name = file.filename if file.filename is not None else "nofilename"

        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            contents = file.file.read()
            sha256 = hashlib.sha256(contents).hexdigest()
            if deduplicate == "True":
                existing_file = (
                    await session.scalars(
                        select(orm.File).where(
                            (orm.File.sha256 == sha256)
                            & (orm.File.file_name == file_name)
                        )
                    )
                ).first()
                if existing_file:
                    return JsonCreateFileOutput(
                        id=existing_file.id,
                        file_name=existing_file.file_name,
                        description=existing_file.description,
                        type_=existing_file.type,
                        size_in_bytes=existing_file.size_in_bytes,
                        original_path=existing_file.original_path,
                    )
            temp_file.write(contents)
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            new_file = create_file_in_db(
                temp_file,
                external_file_name=file_name,
                description=description,
            )

            session.add(new_file)
            await session.commit()

            return JsonCreateFileOutput(
                id=new_file.id,
                file_name=file_name,
                description=description,
                type_=new_file.type,
                size_in_bytes=new_file.size_in_bytes,
                original_path=None,
            )


@router.post(
    "/api/files/simple/{extension}",
    tags=["files"],
    description="""This endpoint was specifically crafted to upload files in a simple way from external scripts, such as the CrystFEL scripts.
    It doesn't need a multipart request and the file extension can be set using the path parameter (which is used to generate nice
    .mtz and .pdb download URLs).
    """,
    response_model_exclude_defaults=True,
)
async def create_file_simple(
    extension: str, request: Request, session: AsyncSession = Depends(get_orm_db)
) -> JsonCreateFileOutput:
    async with session.begin():
        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(await request.body())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            file_name = f"{uuid.uuid4()}.{extension}"

            new_file = create_file_in_db(
                temp_file, external_file_name=file_name, description=""
            )
            session.add(new_file)
            await session.commit()

            return JsonCreateFileOutput(
                id=new_file.id,
                file_name=file_name,
                description="",
                type_=new_file.type,
                size_in_bytes=new_file.size_in_bytes,
                # Doesn't really make sense here
                original_path=None,
            )


@router.get(
    "/api/live-stream/snapshot/{beamtimeId}",
    tags=["events"],
    response_model_exclude_defaults=True,
)
async def create_live_stream_snapshot(
    beamtimeId: int, session: AsyncSession = Depends(get_orm_db)
) -> JsonCreateLiveStreamSnapshotOutput:
    async with session.begin():
        existing_live_stream = (
            await session.scalars(
                select(orm.File).where(
                    orm.File.file_name == live_stream_image_name(beamtimeId)
                )
            )
        ).one()
        new_file_name = live_stream_image_name(beamtimeId) + "-copy"

        new_image = await duplicate_file(existing_live_stream, new_file_name)
        session.add(new_image)
        await session.commit()

        return JsonCreateLiveStreamSnapshotOutput(
            id=new_image.id,
            file_name=new_file_name,
            description=new_image.description,
            type_=new_image.type,
            size_in_bytes=new_image.size_in_bytes,
            original_path=new_image.original_path,
        )


@router.post("/api/live-stream/{beamtimeId}", response_model_exclude_defaults=True)
async def update_live_stream(
    file: UploadFile, beamtimeId: int, session: AsyncSession = Depends(get_orm_db)
) -> JsonUpdateLiveStream:
    async with session.begin():
        # Since we potentially need to seek around in the file, and we don't know if it's a seekable
        # stream (I think?) we store it in a named temp file first.
        with NamedTemporaryFile(mode="w+b") as temp_file:
            temp_file.write(file.file.read())
            temp_file.flush()
            temp_file.seek(0, os.SEEK_SET)

            image_file_name = live_stream_image_name(beamtimeId)
            existing_live_stream = (
                await session.scalars(
                    select(orm.File).where(
                        orm.File.file_name == live_stream_image_name(beamtimeId)
                    )
                )
            ).one_or_none()

            db_file: orm.File
            if existing_live_stream is not None:
                update_file_with_contents(existing_live_stream, temp_file)
                db_file = existing_live_stream
            else:
                db_file = create_file_in_db(
                    temp_file, image_file_name, "Live stream image"
                )
                session.add(db_file)
            await session.commit()
            return JsonUpdateLiveStream(id=db_file.id)


@router.delete("/api/files", tags=["files"], response_model_exclude_defaults=True)
async def delete_file(
    input_: JsonDeleteFileInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonDeleteFileOutput:
    async with session.begin():
        await session.execute(delete(orm.File).where(orm.File.id == input_.id))
        await session.commit()

    return JsonDeleteFileOutput(id=input_.id)


def _do_content_disposition(mime_type: str, extension: str) -> bool:
    if mime_type == "text/plain":
        return extension in ("pdb", "cif")
    return all(
        not mime_type.startswith(x) for x in ("image", "application/pdf", "text/plain")
    )


# This isn't really something the user should call. Rather, it's used in hyperlinks, so we don't include it in the schema.
@router.get("/api/files/{fileId}", tags=["files"], include_in_schema=False)
async def read_file(
    fileId: int, session: AsyncSession = Depends(get_orm_db)
) -> StreamingResponse:
    file_ = (
        await session.scalars(select(orm.File).where(orm.File.id == fileId))
    ).one_or_none()

    if file_ is None:
        raise HTTPException(status_code=404, detail=f"file with id {fileId} not found")

    # No way currently to not load this into memory. Might be an OOM for big files.
    contents_in_memory = await file_.awaitable_attrs.contents

    def async_generator() -> Any:
        yield from BytesIO(contents_in_memory)

    # Content-Disposition makes it so the browser opens a "Save file as" dialog. For images, PDFs, ..., we can just
    # display them in the browser instead.
    return StreamingResponse(
        async_generator(),
        media_type=file_.type,
        headers=(
            {"Content-Disposition": f'attachment; filename="{file_.file_name}"'}
            if _do_content_disposition(file_.type, Path(file_.file_name).suffix[1:])
            else {}
        ),
    )
