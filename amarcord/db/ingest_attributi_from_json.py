from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import attributo_types_semantically_equivalent
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.dbattributo import DBAttributo
from amarcord.json_schema import JSONSchemaUnion


async def ingest_run_attributi_schema(
    db: AsyncDB,
    conn: Connection,
    beamtime_id: BeamtimeId,
    preexisting_attributi: list[DBAttributo],
    attributi_schema: dict[str, tuple[None | str, JSONSchemaUnion]],
    group: str,
) -> None:
    preexisting_attributi_dict: dict[str, DBAttributo] = {
        t.name: t for t in preexisting_attributi
    }
    for attributo_name, (
        attributo_description,
        attributo_schema,
    ) in attributi_schema.items():
        attributo_type = schema_to_attributo_type(attributo_schema)

        existing_attributo = preexisting_attributi_dict.get(attributo_name)
        if existing_attributo is None:
            await db.create_attributo(
                conn,
                beamtime_id=beamtime_id,
                name=attributo_name,
                description=""
                if attributo_description is None
                else attributo_description,
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
