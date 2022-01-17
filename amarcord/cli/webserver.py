import os
import sys

from pint import UnitRegistry
from quart import Quart
from quart_cors import cors
from tap import Tap
from werkzeug.exceptions import HTTPException

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.dbattributo import DBAttributo
from amarcord.json import JSONDict
from amarcord.json_checker import JSONChecker
from amarcord.json_schema import parse_schema_type
from amarcord.quart_utils import CustomJSONEncoder
from amarcord.quart_utils import QuartDatabases
from amarcord.quart_utils import create_quart_standard_error
from amarcord.quart_utils import handle_exception
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
db = QuartDatabases(app)


@app.errorhandler(HTTPException)
def error_handler_for_exceptions(e):
    return handle_exception(e)


@app.post("/api/attributi")
async def create_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.add_attributo(
            conn,
            name=r.retrieve_safe_str("name"),
            description=r.retrieve_safe_str("description"),
            associated_table=AssociatedTable(r.retrieve_safe_str("associatedTable")),
            type_=schema_to_attributo_type(parse_schema_type(r.retrieve_safe("type"))),
        )

    return {}


@app.patch("/api/attributi")
async def change_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        new_attributo = JSONChecker(r.retrieve_safe("newAttributo"), "newAttributo")
        conversion_flags = JSONChecker(
            r.retrieve_safe("conversionFlags"), "conversionFlags"
        )
        await db.instance.change_attributo(
            conn,
            name=r.retrieve_safe_str("nameBefore"),
            conversion_flags=AttributoConversionFlags(
                ignore_units=conversion_flags.retrieve_safe_boolean("ignoreUnits")
            ),
            new_attributo=DBAttributo(
                name=AttributoId(new_attributo.retrieve_safe_str("name")),
                description=new_attributo.retrieve_safe_str("description"),
                associated_table=AssociatedTable(
                    new_attributo.retrieve_safe_str("associatedTable")
                ),
                attributo_type=schema_to_attributo_type(
                    parse_schema_type(new_attributo.retrieve_safe("type"))
                ),
            ),
        )

    return {}


@app.post("/api/unit")
async def check_standard_unit() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    unit_input = r.retrieve_safe_str("input")

    try:
        return {"normalized": f"{UnitRegistry()(unit_input):P}"}
    except:
        return create_quart_standard_error(
            code=None, title="Invalid unit", description=None
        )


@app.delete("/api/attributi")
async def delete_attributo() -> JSONDict:
    r = JSONChecker(await quart_safe_json_dict(), "request")

    async with db.instance.begin() as conn:
        await db.instance.delete_attributo(
            conn,
            name=r.retrieve_safe_str("name"),
        )

    return {}


@app.get("/api/attributi")
async def read_attributi() -> JSONDict:
    async with db.instance.connect() as conn:
        return {
            "attributi": [
                {
                    "name": a.name,
                    "description": a.description,
                    "associatedTable": a.associated_table.value,
                    "type": attributo_type_to_schema(a.attributo_type),
                }
                for a in await db.instance.retrieve_attributi(conn)
            ]
        }


class Arguments(Tap):
    port: int = 5000
    host: str = "0.0.0.0"
    db_url: str = "sqlite+aiosqlite://"
    db_echo: bool = False
    debug: bool = True


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()
    app.config.update({"DB_URL": args.db_url, "DB_ECHO": args.db_echo})
    app.run(host=args.host, port=args.port, debug=args.debug, use_reloader=args.debug)
    return 0


if __name__ == "__main__":
    sys.exit(main())
