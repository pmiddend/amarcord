import datetime
import json
from traceback import format_exception
from typing import Any

from quart import Quart
from quart import request
from quart.json import JSONEncoder

from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import datetime_to_attributo_string
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json_types import JSONDict


async def quart_safe_json_dict() -> JSONDict:
    json_content = await request.get_json(force=True)
    assert isinstance(
        json_content, dict
    ), f"expected a dictionary for the request input, got {json_content}"
    return json_content


class CustomJSONEncoder(JSONEncoder):
    def default(self, object_: Any) -> Any:
        # The default ISO-format for JSON encoding isn't well-parseable, better to use good olde ISO!
        if isinstance(object_, datetime.datetime):
            return datetime_to_attributo_string(object_)
        return JSONEncoder.default(self, object_)


def format_exception_single_string(e: Any) -> str:
    return "\n".join(format_exception(type(e), e, e.__traceback__))


def create_quart_standard_error(
    code: int | None, title: str, description: str | None
) -> JSONDict:
    return {"code": code, "title": title, "description": description}


class CustomWebException(Exception):
    def __init__(self, code: int, title: str, description: str) -> None:
        super().__init__(title)
        self.description = description
        self.title = title
        self.code = code


def handle_exception(e: Any) -> Any:
    """Return JSON instead of HTML for HTTP errors."""
    response = e.get_response()
    if (
        hasattr(e, "original_exception")
        and e.original_exception is not None
        and isinstance(e.original_exception, CustomWebException)
    ):
        response.data = json.dumps(
            {
                "error": create_quart_standard_error(
                    e.original_exception.code,
                    e.original_exception.title,
                    e.original_exception.description,
                )
            }
        )
        # Yes, this looks weird, returning 200 from a failed request. It's, however, a failure of the Elm HTTP framework
        # where you lose the HTTP body if the response code isn't successful. I'm okay with that for now.
        response.status_code = 200
        response.content_type = "application/json"
        return response
    # start with the correct headers and status code from the error
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
        self._instance: AsyncDB | None = None

    async def initialize_db(self) -> None:
        context = AsyncDBContext(
            self._app.config["DB_URL"], self._app.config["DB_ECHO"]
        )
        # pylint: disable=assigning-non-slot
        self._instance = AsyncDB(context, create_tables_from_metadata(context.metadata))
        await self._instance.migrate()

    def init_app(self, app: Quart) -> None:
        app.before_serving(self._before_serving)
        app.after_serving(self._after_serving)

    async def _before_serving(self) -> None:
        await self.initialize_db()

    async def _after_serving(self) -> None:
        if self._instance is not None:
            await self.instance.dispose()

    @property
    def instance(self) -> AsyncDB:
        assert self._instance is not None
        return self._instance
