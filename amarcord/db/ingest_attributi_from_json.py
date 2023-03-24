from typing import Any

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import attributo_types_semantically_equivalent
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.dbattributo import DBAttributo
from amarcord.json_schema import parse_schema_type


async def ingest_run_attributi_schema(
    db: AsyncDB,
    conn: Connection,
    preexisting_attributi: list[DBAttributo],
    attributi_schema: dict[str, Any],
    group: str,
) -> None:
    preexisting_attributi_dict: dict[str, DBAttributo] = {
        t.name: t for t in preexisting_attributi
    }
    for attributo_name, attributo_schema in attributi_schema.items():
        decoded_schema = parse_schema_type(attributo_schema)
        attributo_type = schema_to_attributo_type(decoded_schema)

        existing_attributo = preexisting_attributi_dict.get(attributo_name)
        if existing_attributo is None:
            await db.create_attributo(
                conn,
                name=attributo_name,
                description="",
                group=group,
                type_=attributo_type,
                associated_table=AssociatedTable.RUN,
            )
        else:
            if not attributo_types_semantically_equivalent(
                existing_attributo.attributo_type, attributo_type
            ):
                raise Exception(
                    f"we have a type change, type before: {existing_attributo.attributo_type}, type after: "
                    + str(attributo_type)
                )
