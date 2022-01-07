import os
import sys
from typing import Optional

from quart import Quart
from quart import g
from quart_cors import cors
from tap import Tap

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json import JSONDict
from amarcord.json_checker import JSONChecker
from amarcord.json_schema import parse_schema_type
from amarcord.quart_utils import CustomJSONEncoder
from amarcord.quart_utils import quart_safe_json_dict

app = Quart(
    __name__,
    static_folder=os.environ.get(
        "AMARCORD_STATIC_FOLDER", os.getcwd() + "/frontend/prod"
    ),
    static_url_path="/",
)

app.json_encoder = CustomJSONEncoder
app = cors(app)


@app.before_serving
async def create_db() -> None:
    context = AsyncDBContext(app.config["DB_URL"], app.config["DB_ECHO"])
    # pylint: disable=assigning-non-slot
    g.db = AsyncDB(context, create_tables_from_metadata(context.metadata))


@app.after_serving
async def dispose_db() -> None:
    await g.db.dispose()


def get_db() -> AsyncDB:
    return g.db


@app.route("/")
async def add_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with get_db().connect() as conn:
        await get_db().add_attributo(
            conn,
            name=r.retrieve_safe_str("name"),
            description=r.retrieve_safe_str("description"),
            associated_table=AssociatedTable(r.retrieve_safe_str("associatedTable")),
            type_=schema_to_attributo_type(parse_schema_type(r.retrieve_safe("type"))),
        )

    return {}


class Arguments(Tap):
    port: int
    host: str = "0.0.0.0"
    db_url: Optional[str]
    db_echo: bool = False
    debug: bool = False


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()
    app.config.update({"DB_URL": args.db_url, "DB_ECHO": args.db_echo})
    app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())
