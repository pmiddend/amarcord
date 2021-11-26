import datetime
import logging
from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from typing import Any
from typing import Dict
from typing import Final
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from kamzik3.constants import ATTR_STATUS
from kamzik3.constants import STATUS_BUSY
from kamzik3.constants import STATUS_DISCONNECTED
from kamzik3.constants import STATUS_DISCONNECTING
from kamzik3.constants import STATUS_IDLE
from kamzik3.constants import VALUE
from kamzik3.devices.deviceClient import DeviceClient
from pint import Quantity

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_id import sanitize_attributo_id
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.pint_util import pint_quantity_to_attributo_type

STOPPED_ATTRIBUTO_ID = AttributoId("stopped")

STARTED_ATTRIBUTO_ID = AttributoId("started")

ATTR_RUN_ID: Final = "Run id"
ATTR_METADATA: Final = "Metadata"

DO_CLOSE_PRIOR_RUNS = False

logger = logging.getLogger(__name__)


class _BeamlineStatus(Enum):
    BUSY = STATUS_BUSY
    IDLE = STATUS_IDLE
    DISCONNECTED = STATUS_DISCONNECTED
    DISCONNECTING = STATUS_DISCONNECTING


_RunMetadataValue = Union[str, int, float, Quantity]
_RunMetadata = Dict[str, _RunMetadataValue]


@dataclass(frozen=True, eq=True)
class _InputStatusChange:
    status: _BeamlineStatus


@dataclass(frozen=True, eq=True)
class _InputNewRun:
    run_id: int
    metadata: _RunMetadata


@dataclass(frozen=True, eq=True)
class _OutputAddRun:
    run_id: int
    started: datetime.datetime
    metadata: _RunMetadata


@dataclass(frozen=True, eq=True)
class _OutputCloseRun:
    run_id: int
    stopped: datetime.datetime


@dataclass(frozen=True, eq=True)
class _OutputClosePriorRuns:
    run_id: Optional[int]


_BeamlineInput = Union[_InputNewRun, _InputStatusChange]
_BeamlineOutput = Union[_OutputAddRun, _OutputCloseRun, _OutputClosePriorRuns]


@dataclass(frozen=True, eq=True)
class _State:
    run_id: Optional[int]
    run_finished: bool


@dataclass(frozen=True, eq=True)
class _StateOutput:
    new_state: _State
    outputs: List[_BeamlineOutput]


def _init_state_machine() -> _StateOutput:
    return _StateOutput(
        new_state=_State(run_id=None, run_finished=False),
        outputs=[_OutputClosePriorRuns(None)],
    )


def _step_state_machine(state: _State, input_: _BeamlineInput) -> _StateOutput:
    if state.run_id is None:
        if isinstance(input_, _InputStatusChange):
            if input_.status == _BeamlineStatus.IDLE:
                logger.info(
                    "no run ID, but got status change to idle, closing prior runs"
                )
                return _StateOutput(
                    new_state=state, outputs=[_OutputClosePriorRuns(None)]
                )
            logger.info(f"no run ID, but got status change: {input_}")
            return _StateOutput(new_state=state, outputs=[])
        logger.info(f"got our first run ID: {input_.run_id}")
        # has to be run ID now
        return _StateOutput(
            new_state=replace(state, run_id=input_.run_id, run_finished=False),
            outputs=[
                _OutputClosePriorRuns(input_.run_id),
                _OutputAddRun(
                    input_.run_id, datetime.datetime.utcnow(), input_.metadata
                ),
            ],
        )
    if isinstance(input_, _InputStatusChange):
        if (
            input_.status == _BeamlineStatus.DISCONNECTED
            or input_.status == _BeamlineStatus.DISCONNECTING
        ):
            logger.info(f"disconnected, clearing run ID {state.run_id}")
            return _StateOutput(new_state=replace(state, run_id=None), outputs=[])
        if input_.status == _BeamlineStatus.IDLE:
            logger.info(f"idle, closing run {state.run_id}")
            return _StateOutput(
                new_state=replace(state, run_id=None, run_finished=True),
                outputs=[_OutputCloseRun(state.run_id, datetime.datetime.now())],
            )
        # has to be BUSY now
        # usually we get a run and then the busy signal, but we just assume the run is busy
        # when the new run ID comes in
        return _StateOutput(new_state=state, outputs=[])
    assert isinstance(input_, _InputNewRun)
    outputs: List[_BeamlineOutput] = []
    if not state.run_finished:
        logger.warning(
            f"got new run ID {input_.run_id}, but old run {state.run_id} not finished",
        )
        outputs.append(_OutputCloseRun(state.run_id, datetime.datetime.utcnow()))
    else:
        logger.info("got new run ID {input_.run_id}")
    outputs.append(
        _OutputAddRun(input_.run_id, datetime.datetime.utcnow(), input_.metadata)
    )
    return _StateOutput(
        new_state=replace(state, run_finished=False, run_id=input_.run_id),
        outputs=outputs,
    )


def _resolve_type(x: _RunMetadataValue) -> Optional[AttributoType]:
    if isinstance(x, int):
        return AttributoTypeInt()
    if isinstance(x, float):
        return AttributoTypeDouble()
    if isinstance(x, str):
        return AttributoTypeString()
    if isinstance(x, Quantity):
        return pint_quantity_to_attributo_type(x)
    return None


class KamzikClient(DeviceClient):
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

        self.database_url = database_url
        self.proposal_id = proposal_id

        dbcontext = DBContext(database_url, echo=False)
        self._db = DB(dbcontext, create_tables(dbcontext))
        self._add_initial_attributi()
        init_state_output = _init_state_machine()
        self._state = init_state_output.new_state
        for o in init_state_output.outputs:
            self._process_output(o)

        logger.info("kamzik client initialized")

        # Deliberately at the bottom here, since kamzik otherwise sends us events while the constructor
        # is still running.
        super().__init__(host, port, device_id, config)

    def _possibly_add_attributo(
        self,
        conn: Connection,
        attributi: Dict[AttributoId, DBAttributo],
        name: str,
        value: _RunMetadataValue,
    ) -> bool:
        attributo_id = sanitize_attributo_id(name)

        if attributo_id in attributi:
            return True

        resolved_type = _resolve_type(value)

        if resolved_type is None:
            return False

        self._db.add_attributo(
            conn,
            attributo_id,
            name,
            AssociatedTable.RUN,
            resolved_type,
        )
        return True

    def _process_output(self, o: _BeamlineOutput) -> None:
        if isinstance(o, _OutputCloseRun):
            with self._db.connect() as conn:
                self._db.update_run_attributo(
                    conn,
                    o.run_id,
                    STOPPED_ATTRIBUTO_ID,
                    o.stopped,
                    ONLINE_SOURCE_NAME,
                )
        elif isinstance(o, _OutputAddRun):
            with self._db.connect() as conn:
                existing_attributi = self._db.retrieve_table_attributi(
                    conn, AssociatedTable.RUN
                )

                resolved_metadata_keys: Set[str] = set()

                # we make two passes here, one to add attributi, and then one to set them
                for k, v in o.metadata.items():
                    if self._possibly_add_attributo(conn, existing_attributi, k, v):
                        resolved_metadata_keys.add(k)

                existing_attributi = self._db.retrieve_table_attributi(
                    conn, AssociatedTable.RUN
                )
                attributi = AttributiMap(existing_attributi)
                attributi.append_single_to_source(
                    ONLINE_SOURCE_NAME, STARTED_ATTRIBUTO_ID, datetime.datetime.utcnow()
                )
                for k, v in o.metadata.items():
                    if k in resolved_metadata_keys:
                        resolved_value = v.magnitude if isinstance(v, Quantity) else v
                        attributi.append_single_to_source(
                            ONLINE_SOURCE_NAME, sanitize_attributo_id(k), resolved_value
                        )

                self._db.add_run(
                    conn,
                    ProposalId(self.proposal_id),
                    run_id=o.run_id,
                    sample_id=None,
                    attributi=attributi.to_raw(),
                )
        else:
            assert isinstance(o, _OutputClosePriorRuns), f"output was {o}"

            if DO_CLOSE_PRIOR_RUNS:
                with self._db.connect() as conn:
                    existing_attributi = self._db.retrieve_table_attributi(
                        conn, AssociatedTable.RUN
                    )

                    runs_with_stopped: List[
                        Tuple[int, Optional[datetime.datetime]]
                    ] = []
                    for run in self._db.retrieve_runs(
                        conn, ProposalId(self.proposal_id), since=None
                    ):
                        attributi = AttributiMap(existing_attributi, run.attributi)
                        runs_with_stopped.append(
                            (run.id, attributi.select_datetime(STOPPED_ATTRIBUTO_ID))
                        )

                    number_of_runs = len(runs_with_stopped)
                    for i in range(number_of_runs):
                        run_id, stopped = runs_with_stopped[i]

                        if o.run_id is not None and run_id >= o.run_id:
                            continue

                        if stopped is None:
                            logger.info(f"closing (prior) run {run_id}")

                            # the last run we have isn't stopped, then stop now
                            # otherwise, stop at the beginning of next run, keeping things at least monotonous
                            self._db.update_run_attributo(
                                conn,
                                run_id,
                                STOPPED_ATTRIBUTO_ID,
                                datetime.datetime.utcnow()
                                if i == number_of_runs - 1
                                else runs_with_stopped[i + 1][1],
                                ONLINE_SOURCE_NAME,
                            )

    def _add_initial_attributi(self):
        with self._db.connect() as conn:
            attributi = self._db.retrieve_table_attributi(
                conn, AssociatedTable.RUN, inherent=False
            )
            if STARTED_ATTRIBUTO_ID not in attributi:
                logger.info(f"attributo {STARTED_ATTRIBUTO_ID} not present, adding")
                self._db.add_attributo(
                    conn,
                    "started",
                    "Started",
                    AssociatedTable.RUN,
                    AttributoTypeDateTime(),
                )
            if STOPPED_ATTRIBUTO_ID not in attributi:
                logger.info(f"attributo {STOPPED_ATTRIBUTO_ID} not present, adding")
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
            ATTR_STATUS, callback=self._event_status_changed, key_filter=VALUE
        )
        self.attach_attribute_callback(
            ATTR_METADATA, callback=self._event_metadata_changed, key_filter=VALUE  # type: ignore
        )
        self.configured = True

    def _process_input(self, input_: _BeamlineInput) -> None:
        state_output = _step_state_machine(self._state, input_)

        self._state = state_output.new_state

        for o in state_output.outputs:
            self._process_output(o)

    def _event_metadata_changed(self, metadata: _RunMetadata) -> None:
        """
        Called when run_id was changed.
        Method is always called at least once, when attached to the attribute.
        """
        RUN_ID_KEY: Final = "Run id"

        # happens on first start
        if metadata == {}:
            logger.info("got empty metadata dict")
            return

        run_id = metadata.get(RUN_ID_KEY, None)
        assert isinstance(
            run_id, int
        ), f"Run ID is not int, but {run_id} in metadata dict {metadata}"

        metadata_without_run_id = {k: v for k, v in metadata.items() if k != RUN_ID_KEY}

        self._process_input(_InputNewRun(run_id, metadata_without_run_id))

    def _add_run(self, conn: Connection, run_id: int) -> None:
        attributi_map = RawAttributiMap({})
        attributi_map.append_single_datetime_to_source(
            ONLINE_SOURCE_NAME, STARTED_ATTRIBUTO_ID, datetime.datetime.utcnow()
        )
        run_is_new = self._db.add_run(
            conn,
            ProposalId(self.proposal_id),
            run_id,
            sample_id=None,
            attributi=attributi_map,
        )
        if not run_is_new:
            logger.info(f"run {run_id} already existed, did nothing.")

    def _add_event(self, level: EventLogLevel, message: str) -> None:
        if level == EventLogLevel.INFO:
            logger.info("%s", message)
        elif level == EventLogLevel.WARNING:
            logger.warning("%s", message)
        elif level == EventLogLevel.ERROR:
            logger.error("%s", message)
        with self._db.connect() as conn:
            self._db.add_event(conn, level, "kamzik", message)

    def _event_status_changed(self, status: str) -> None:
        if status == STATUS_BUSY:
            self._add_event(EventLogLevel.INFO, "Status change: busy")
            self._process_input(_InputStatusChange(_BeamlineStatus.BUSY))
        elif status == STATUS_IDLE:
            self._add_event(EventLogLevel.INFO, "Status change: idle")
            self._process_input(_InputStatusChange(_BeamlineStatus.IDLE))
        elif status == STATUS_DISCONNECTING:
            self._add_event(EventLogLevel.WARNING, "Status change: disconnecting")
            self._process_input(_InputStatusChange(_BeamlineStatus.DISCONNECTING))
        elif status == STATUS_DISCONNECTED:
            self._add_event(EventLogLevel.WARNING, "Status change: disconnected")
            self._process_input(_InputStatusChange(_BeamlineStatus.DISCONNECTED))

            # Detach callbacks, not necessary, but it helps status handling simple
            self.detach_attribute_callback(ATTR_METADATA, callback=self._event_metadata_changed)  # type: ignore
            self.detach_attribute_callback(
                ATTR_STATUS, callback=self._event_status_changed
            )
