import datetime
from io import BytesIO
from pathlib import Path
from typing import Generator
from zipfile import ZipFile

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.excel_export import create_workbook
from amarcord.web.fastapi_utils import get_orm_db

router = APIRouter()


# See https://stackoverflow.com/questions/55873174/how-do-i-return-an-image-in-fastapi
@router.get(
    "/api/{beamtimeId}/spreadsheet.zip",
    responses={200: {"content": {"application/zip": {}}}},
    include_in_schema=False,
)
async def download_spreadsheet(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
) -> Response:
    workbook_output = await create_workbook(session, beamtimeId, with_events=True)
    workbook = workbook_output.workbook
    workbook_bytes = BytesIO()
    workbook.save(workbook_bytes)
    zipfile_bytes = BytesIO()
    with ZipFile(zipfile_bytes, "w") as result_zip:
        dirname = "amarcord-output-" + datetime.datetime.utcnow().strftime(
            "%Y-%m-%d_%H-%M-%S"
        )
        result_zip.writestr(f"{dirname}/tables.xlsx", workbook_bytes.getvalue())
        for file_ in workbook_output.files:
            result_zip.writestr(
                f"{dirname}/files/{file_.id}" + Path(file_.file_name).suffix,
                file_.contents,
            )
    zipfile_bytes.seek(0)

    def iterzipfile() -> Generator[bytes, None, None]:
        yield from zipfile_bytes

    return StreamingResponse(
        iterzipfile(),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{dirname}.zip"'},
    )
