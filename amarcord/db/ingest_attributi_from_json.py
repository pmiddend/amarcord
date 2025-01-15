from sqlalchemy.ext.asyncio import AsyncSession

from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import attributo_types_semantically_equivalent
from amarcord.db.attributi import coparse_schema_type
from amarcord.db.attributi import schema_union_to_attributo_type
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.json_schema import JSONSchemaUnion


def ingest_run_attributi_schema(
    session: AsyncSession,
    beamtime_id: BeamtimeId,
    preexisting_attributi_by_name: dict[str, AttributoType],
    attributi_schema: dict[str, tuple[None | str, JSONSchemaUnion]],
    group: str,
) -> None:
    for attributo_name, (
        attributo_description,
        attributo_schema,
    ) in attributi_schema.items():
        attributo_type = schema_union_to_attributo_type(attributo_schema)

        existing_attributo_type = preexisting_attributi_by_name.get(attributo_name)
        if existing_attributo_type is None:
            session.add(
                orm.Attributo(
                    beamtime_id=beamtime_id,
                    name=attributo_name,
                    description=(
                        "" if attributo_description is None else attributo_description
                    ),
                    group=group,
                    associated_table=AssociatedTable.RUN,
                    json_schema=coparse_schema_type(
                        attributo_type_to_schema(attributo_type),
                    ),
                ),
            )
        elif not attributo_types_semantically_equivalent(
            existing_attributo_type,
            attributo_type,
        ):
            raise Exception(
                f"we have a type change, type before: {existing_attributo_type}, type after: "
                + str(attributo_type),
            )
