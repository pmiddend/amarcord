import asyncio
import logging
from typing import Any
from typing import Dict
from typing import Final

from kamzik3.constants import ATTR_STATUS
from kamzik3.constants import VALUE
from kamzik3.devices.deviceClient import DeviceClient

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import schema_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.dbcontext import Connection
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json_schema import parse_schema_type

RUN_STATUS_STOPPED = "stopped"

RUN_STATUS_RUNNING = "running"

RUN_STATUS_UNKNOWN = "unknown"

STOPPED_ATTRIBUTO_ID = "stopped"

STARTED_ATTRIBUTO_ID = "started"

KAMZIK_ATTRIBUTO_GROUP: Final = "kamzik"

ATTR_RUN_ID: Final = "Run id"
ATTR_METADATA: Final = "Metadata"

DO_CLOSE_PRIOR_RUNS = False

logger = logging.getLogger(__name__)


async def process_kamzik_metadata(
    db: AsyncDB,
    conn: Connection,
    run_id: int,
    attributi_schema: Dict[str, Any],
    attributi_values: Dict[str, Any],
) -> None:
    preexisting_attributi = {
        t.name: t
        for t in await db.retrieve_attributi(conn, associated_table=AssociatedTable.RUN)
    }

    for attributo_name, attributo_schema in attributi_schema.items():
        decoded_schema = parse_schema_type(attributo_schema)
        attributo_type = schema_to_attributo_type(decoded_schema)

        existing_attributo = preexisting_attributi.get(attributo_name, None)
        if existing_attributo is None:
            await db.create_attributo(
                conn,
                name=attributo_name,
                description="",
                group=KAMZIK_ATTRIBUTO_GROUP,
                type_=attributo_type,
                associated_table=AssociatedTable.RUN,
            )
        else:
            if existing_attributo.attributo_type != attributo_type:
                raise Exception(
                    f"we have a type change, type before: {existing_attributo.attributo_type}, type after: {attributo_type}"
                )

    attributi = await db.retrieve_attributi(conn, associated_table=AssociatedTable.RUN)
    existing_run = await db.retrieve_run(conn, run_id, attributi)
    attributi_map = (
        existing_run.attributi
        if existing_run is not None
        else AttributiMap.from_types_and_json(
            attributi,
            sample_ids=[],
            raw_attributi={},
        )
    )
    for attributo_name, value in attributi_values.items():
        attributi_map.append_single(attributo_name, value)
    if existing_run is not None:
        await db.update_run_attributi(conn, run_id, attributi_map)
    else:
        await db.create_run(conn, run_id, attributi_map)


def _event_status_changed(status: str) -> None:
    logger.info(f"status changed to {status}")


class KamzikClient(DeviceClient):
    configured = False

    def __init__(
        self,
        host: str,
        port: int,
        database_url: str,
        device_id: Any = None,
        config: Any = None,
    ) -> None:
        # Kamzik doesn't do type-checking on the parameters, so we do it, just to be sure
        assert isinstance(database_url, str)

        self.database_url = database_url

        db_context = AsyncDBContext(database_url, echo=False)
        self._db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))

        logger.info("kamzik client initialized")

        # Deliberately at the bottom here, since kamzik otherwise sends us events while the constructor
        # is still running.
        super().__init__(host, port, device_id, config)

    def handle_configuration(self):
        super().handle_configuration()
        logger.info("client is connected...")
        self.attach_attribute_callback(
            ATTR_STATUS, callback=_event_status_changed, key_filter=VALUE
        )
        self.attach_attribute_callback(
            ATTR_METADATA, callback=self._event_metadata_changed, key_filter=VALUE  # type: ignore
        )
        self.configured = True

    async def _process_kamzik_metadata(
        self,
        run_id: int,
        attributi_schema: Dict[str, Any],
        attributi_values: Dict[str, Any],
    ) -> None:
        async with self._db.begin() as conn:
            await process_kamzik_metadata(
                self._db, conn, run_id, attributi_schema, attributi_values
            )

    def _event_metadata_changed(self, metadata: Dict[str, Any]) -> None:
        """
        Called when run_id was changed.
        Method is always called at least once, when attached to the attribute.
        """
        # happens on first start
        if metadata == {}:
            logger.info("got empty metadata dict")
            return

        run_id = metadata.get("run_id", None)
        assert run_id is not None, f"got no run ID in metadata dict {metadata}"
        assert isinstance(
            run_id, int
        ), f"Run ID is not int, but {run_id} in metadata dict {metadata}"

        attributi_schema = metadata.get("attributi-schema", None)
        assert (
            attributi_schema is not None
        ), f"got no attributi-schema in metadata dict {metadata}"
        assert isinstance(
            attributi_schema, dict
        ), f"got no attributi-schema dict in metadata dict {metadata}"

        attributi_values = metadata.get("attributi-values", None)
        assert isinstance(
            attributi_values, dict
        ), f"got no attributi-values in metadata dict {metadata}"

        asyncio.run(
            self._process_kamzik_metadata(run_id, attributi_schema, attributi_values)
        )
