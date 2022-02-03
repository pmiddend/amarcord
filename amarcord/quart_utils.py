import datetime
import json
from traceback import format_exception
from typing import Any
from typing import Optional

from quart import Quart
from quart import request
from quart.json import JSONEncoder

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import datetime_to_attributo_string
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json import JSONDict


async def quart_safe_json_dict() -> JSONDict:
    json_content = await request.get_json(force=True)
    assert isinstance(
        json_content, dict
    ), f"expected a dictionary for the request input, got {json_content}"
    return json_content


class CustomJSONEncoder(JSONEncoder):
    def default(self, object_):
        # The default ISO-format for JSON encoding isn't well-parseable, better to use good olde ISO!
        if isinstance(object_, datetime.datetime):
            return datetime_to_attributo_string(object_)
        return JSONEncoder.default(self, object_)


def format_exception_single_string(e: Any) -> str:
    return "\n".join(format_exception(type(e), e, e.__traceback__))


def create_quart_standard_error(
    code: Optional[int], title: str, description: Optional[str]
) -> JSONDict:
    return {"code": code, "title": title, "description": description}


def handle_exception(e):
    """Return JSON instead of HTML for HTTP errors."""
    # start with the correct headers and status code from the error
    response = e.get_response()
    # replace the body with JSON
    response.data = json.dumps(
        {
            "error": create_quart_standard_error(
                e.code,
                e.name,
                "original exception: "
                + format_exception_single_string(e.original_exception)
                if hasattr(e, "original_exception") and e.original_exception is not None
                else "no original exception",
            )
        }
    )
    response.content_type = "application/json"
    return response


class QuartDatabases:
    def __init__(self, app: Quart) -> None:
        self.init_app(app)
        self._app = app
        self._instance: Optional[AsyncDB] = None

    def init_app(self, app: Quart) -> None:
        app.before_serving(self._before_serving)
        app.after_serving(self._after_serving)

    async def _before_serving(self) -> None:
        context = AsyncDBContext(
            self._app.config["DB_URL"], self._app.config["DB_ECHO"]
        )
        # pylint: disable=assigning-non-slot
        self._instance = AsyncDB(context, create_tables_from_metadata(context.metadata))
        await context.create_all(CreationMode.CHECK_FIRST)

    async def _after_serving(self) -> None:
        if self._instance is not None:
            await self.instance.dispose()

    @property
    def instance(self) -> AsyncDB:
        assert self._instance is not None
        return self._instance
