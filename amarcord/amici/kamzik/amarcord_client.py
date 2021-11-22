import datetime
import logging
from typing import Any
from typing import Final
from typing import Optional

from kamzik3.constants import ATTR_STATUS
from kamzik3.constants import STATUS_BUSY
from kamzik3.constants import STATUS_DISCONNECTED
from kamzik3.constants import STATUS_DISCONNECTING
from kamzik3.constants import STATUS_IDLE
from kamzik3.constants import VALUE
from kamzik3.devices.deviceClient import DeviceClient

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext

STOPPED_ATTRIBUTO_ID = AttributoId("stopped")

STARTED_ATTRIBUTO_ID = AttributoId("started")

ATTR_RUN_ID: Final = "Run id"

logger = logging.getLogger(__name__)


class AmarcordClient(DeviceClient):
    configured = False

    def __init__(
        self,
        host: str,
        port: int,
        database_url: str,
        proposal_id: int,
        device_id: Any = None,
        config: Any = None,
    ) -> None:
        # Kamzik doesn't do type-checking on the parameters, so we do it, just to be sure
        assert isinstance(database_url, str)
        assert isinstance(proposal_id, int)

        self._database_url = database_url
        self._proposal_id = proposal_id
        dbcontext = DBContext(database_url, echo=False)
        self._db = DB(dbcontext, create_tables(dbcontext))
        self._current_run: Optional[int] = None
        self._add_initial_attributi()

        logger.info("kamzik client initialized")

        # Deliberately at the bottom here, since kamzik otherwise sends us events while the constructor
        # is still running.
        super().__init__(host, port, device_id, config)

    def _add_initial_attributi(self):
        with self._db.connect() as conn:
            attributi = self._db.retrieve_table_attributi(
                conn, AssociatedTable.RUN, inherent=False
            )
            if STARTED_ATTRIBUTO_ID not in attributi:
                logger.info("attributo %s not present, adding", STARTED_ATTRIBUTO_ID)
                self._db.add_attributo(
                    conn,
                    "started",
                    "Started",
                    AssociatedTable.RUN,
                    AttributoTypeDateTime(),
                )
            if STOPPED_ATTRIBUTO_ID not in attributi:
                logger.info("attributo %s not present, adding", STOPPED_ATTRIBUTO_ID)
                self._db.add_attributo(
                    conn,
                    "stopped",
                    "Stopped",
                    AssociatedTable.RUN,
                    AttributoTypeDateTime(),
                )

    def handle_configuration(self):
        super().handle_configuration()
        logger.info("client is connected...")
        self.attach_attribute_callback(
            ATTR_RUN_ID, callback=self.event_new_run, key_filter=VALUE  # type: ignore
        )
        self.attach_attribute_callback(
            ATTR_STATUS, callback=self.event_status_changed, key_filter=VALUE
        )
        self.configured = True

    def event_new_run(self, run_id: Any) -> None:
        """
        Called when run_id was changed.
        Method is always called at least once, when attached to the attribute.
        """
        assert isinstance(run_id, int)

        with self._db.connect() as conn:
            logger.info("adding run %s...", run_id)
            self._add_run(conn, run_id)
            self._current_run = run_id

    def _add_run(self, conn: Connection, run_id: int) -> None:
        attributi_map = RawAttributiMap({})
        attributi_map.append_single_datetime_to_source(
            ONLINE_SOURCE_NAME, STARTED_ATTRIBUTO_ID, datetime.datetime.utcnow()
        )
        run_is_new = self._db.add_run(
            conn,
            ProposalId(self._proposal_id),
            run_id,
            sample_id=None,
            attributi=attributi_map,
        )
        if not run_is_new:
            logger.info("run %s already existed, did nothing.", run_id)

    def _add_event(self, level: EventLogLevel, message: str) -> None:
        if level == EventLogLevel.INFO:
            logger.info("%s", message)
        elif level == EventLogLevel.WARNING:
            logger.warning("%s", message)
        elif level == EventLogLevel.ERROR:
            logger.error("%s", message)
        with self._db.connect() as conn:
            self._db.add_event(conn, level, "kamzik", message)

    def event_status_changed(self, status: str) -> None:
        """
        Called when Device status has changed.
        When Device is Busy run is in progress.
        When Device is Idle run was stopped.
        Method is always called at least once, when attached to the attribute.
        """
        if status == STATUS_BUSY:
            logger.info("Run is active")
        elif status == STATUS_IDLE:
            if self._current_run is None:
                logger.warning("got idle signal, but have no run ID, ignoring")
            else:
                logger.info("idle signal, stopping run %s", self._current_run)
                with self._db.connect() as conn:
                    self._db.update_run_attributo(
                        conn,
                        self._current_run,
                        STOPPED_ATTRIBUTO_ID,
                        datetime.datetime.utcnow(),
                        ONLINE_SOURCE_NAME,
                    )
        elif status == STATUS_DISCONNECTING:
            self._add_event(EventLogLevel.WARNING, "Server is closing connection")
        elif status == STATUS_DISCONNECTED:
            self._add_event(
                EventLogLevel.WARNING,
                "Connection with server closed, waiting to reconnect...",
            )
            # Detach callbacks, not necessary, but it helps status handling simple
            self.detach_attribute_callback(ATTR_RUN_ID, callback=self.event_new_run)  # type: ignore
            self.detach_attribute_callback(
                ATTR_STATUS, callback=self.event_status_changed
            )
