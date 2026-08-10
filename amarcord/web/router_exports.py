import datetime
import time
from typing import Annotated
from typing import AsyncIterable

import anyio
import structlog
from anyio.streams.file import FileReadStream
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select
from starlette.datastructures import FormData
from starlette.datastructures import UploadFile

from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.import_export_db import import_db_from_zip
from amarcord.util import temporary_env
from amarcord.web.fastapi_utils import get_db_url
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.import_export_settings import parse_import_export_settings
from amarcord.web.json_models import JsonExportJob
from amarcord.web.json_models import JsonExportJobInput
from amarcord.web.json_models import JsonExportJobOutput
from amarcord.web.json_models import JsonImportJobOutput
from amarcord.web.json_models import JsonReadExportJobs

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


async def form_body_with_large_limits(request: Request) -> FormData:
    export_settings = parse_import_export_settings()
    if export_settings.import_directory is None:
        raise HTTPException(
            status_code=403,
            detail="Import was disabled by the administrator. Check the documentation on how to enable it.",
        )
    # This is a terrible solution, but starlette really leaves us no choice.
    with temporary_env("TMPDIR", str(export_settings.import_directory)):
        return await request.form(max_part_size=500 * 1024 * 1024 * 1024)


# Because the OpenAPI schema for this would be useless anyways (since we have no types to indicate parameters) we exclude this altogether
@router.post(
    "/api/exports/import",
    tags=["exports"],
    response_model_exclude_defaults=True,
    include_in_schema=False,
)
async def create_import_job(
    form_data: Annotated[FormData, Depends(form_body_with_large_limits)],
) -> JsonImportJobOutput:
    logger.info("Inside import job function.")
    export_settings = parse_import_export_settings()
    if export_settings.import_directory is None:
        raise HTTPException(
            status_code=403,
            detail="Import was disabled by the administrator. Check the documentation on how to enable it.",
        )
    try:
        logger.info("Starting import.")
        file: UploadFile | str | None = form_data.get("file")
        assert file is not None
        assert not isinstance(file, str)
        new_title = form_data.get("new_title")
        assert new_title is None or isinstance(new_title, str)
        new_output_path = form_data.get("new_output_path")
        assert new_output_path is None or isinstance(new_output_path, str)
        if not new_output_path:
            new_output_path = str(export_settings.import_directory)
        number_of_indexing_results = await import_db_from_zip(
            file_obj=file.file,
            file_name=file.filename,
            stream_file_dir=anyio.Path(export_settings.import_directory),
            import_to_db_connection_url=get_db_url(),
            new_title=new_title or None,
            new_output_path=new_output_path or None,
        )
    except Exception as e:
        logger.exception("Error while uploading zip file.")
        raise HTTPException(status_code=400, detail=str(e))
    return JsonImportJobOutput(
        status_message=f"Done. Imported {number_of_indexing_results} indexing result(s)."
    )


@router.post("/api/exports/{beamtimeId}", tags=["exports"])
async def create_export_job(
    input_: JsonExportJobInput,
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonExportJobOutput:
    export_settings = parse_import_export_settings()
    if export_settings.export_directory is None:
        raise HTTPException(status_code=400, detail="Exporting is disabled.")
    new_job = orm.ExportJob(
        beamtime_id=beamtimeId,
        created=datetime.datetime.now(datetime.UTC),
        started=None,
        stopped=None,
        size_in_mebibytes=0,
        output_path=str(
            export_settings.export_directory
            / f"amarcord-export-{beamtimeId}-{int(time.time())}.zip"
        ),
        status_message="",
        contains_stream_files=input_.with_stream_files,
    )
    session.add(new_job)
    await session.commit()
    return JsonExportJobOutput(export_job_id=new_job.id)


@router.get(
    "/api/exports/{beamtimeId}", tags=["exports"], response_model_exclude_defaults=True
)
async def read_exports(
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadExportJobs:
    export_settings = parse_import_export_settings()
    if export_settings.export_directory is None:
        return JsonReadExportJobs(export_base_path=None, export_jobs=[])
    return JsonReadExportJobs(
        export_base_path=str(export_settings.export_directory),
        export_jobs=[
            JsonExportJob(
                id=job.id,
                beamtime_id=beamtimeId,
                created=utc_datetime_to_utc_int(job.created),
                created_local=utc_datetime_to_local_int(job.created),
                started=utc_datetime_to_utc_int(job.started)
                if job.started is not None
                else None,
                started_local=utc_datetime_to_local_int(job.started)
                if job.started is not None
                else None,
                stopped=utc_datetime_to_utc_int(job.stopped)
                if job.stopped is not None
                else None,
                stopped_local=utc_datetime_to_local_int(job.stopped)
                if job.stopped is not None
                else None,
                size_in_mebibytes=job.size_in_mebibytes,
                output_path=job.output_path,
                status_message=job.status_message,
            )
            for job in await session.scalars(
                select(orm.ExportJob)
                .where(orm.ExportJob.beamtime_id == beamtimeId)
                .order_by(orm.ExportJob.id.desc())
            )
        ],
    )


class ZipStreamingResponse(StreamingResponse):
    media_type = "application/zip"


@router.get(
    "/api/exports/{beamtimeId}/{exportId}.zip",
    tags=["exports"],
    response_model_exclude_defaults=True,
    response_class=ZipStreamingResponse,
)
async def read_single_export(
    beamtimeId: BeamtimeId,  # noqa: N803,ARG001
    exportId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> AsyncIterable[bytes]:
    export = await session.get(orm.ExportJob, exportId)
    if export is None:
        raise HTTPException(
            status_code=404, detail=f"No export job with ID {exportId} found."
        )
    if export.output_path is None or export.stopped is None:
        raise HTTPException(
            status_code=400,
            detail=f"Export job {exportId} is not finished.",
        )
    if not await anyio.Path(export.output_path).is_file():
        raise HTTPException(
            status_code=400,
            detail=f"Export job {exportId} does not have a valid zip file output.",
        )
    async with await FileReadStream.from_path(anyio.Path(export.output_path)) as stream:
        async for chunk in stream:
            yield chunk
