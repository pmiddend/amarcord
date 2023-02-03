import asyncio
import datetime
import logging
import random
from pathlib import Path

import randomname
import structlog
from randomname import generate
from structlog.stdlib import BoundLogger
from tap import Tap

from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributi_map import UntypedAttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_name_and_role import AttributoNameAndRole
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.table_classes import DBRun
from amarcord.numeric_range import NumericRange
from amarcord.util import first
from amarcord.util import safe_max

TIME_RESOLVED = AttributoId("time-resolved")

CHEMICAL_BASED = AttributoId("chemical-based")

ATTRIBUTO_TRASH = AttributoId("trash")

ATTRIBUTO_COMMENT = AttributoId("comment")

ATTRIBUTO_FLOW_RATE = AttributoId("flow_rate")

ATTRIBUTO_PH = AttributoId("pH")

ATTRIBUTO_FRAME_TIME = AttributoId("frame_time")

ATTRIBUTO_TARGET_FRAME_COUNT = AttributoId("target_frame_count")

ATTRIBUTO_CHEMICAL = AttributoId("chemical")

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = structlog.get_logger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    verbose: bool = False  # Show more log messages
    wait_time_seconds: float  # How long to wait before simulating another event (in seconds)


def random_date(start: datetime.datetime, end: datetime.datetime) -> datetime.datetime:
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start + datetime.timedelta(seconds=random_second)


def _generate_attributo_value(
    a: AttributoType, chemical_ids: list[int]
) -> AttributoValue:
    if isinstance(a, AttributoTypeInt):
        return random.randint(-300, 300)
    if isinstance(a, AttributoTypeChoice):
        return random.choice(a.values)
    if isinstance(a, AttributoTypeChemical):
        return random.choice(chemical_ids)
    if isinstance(a, AttributoTypeString):
        return generate("n/*")  # type: ignore
    if isinstance(a, AttributoTypeBoolean):
        return random.random() >= 0.5
    if isinstance(a, AttributoTypeDecimal):
        if a.range is None:
            return random.uniform(-1000, 1000)
        if a.range.minimum is not None and a.range.maximum is not None:
            return random.uniform(a.range.minimum, a.range.maximum)
        if a.range.minimum is not None:
            return random.uniform(a.range.minimum, a.range.minimum + 1000)
        if a.range.maximum is not None:
            return random.uniform(a.range.maximum - 1000, a.range.maximum)
        return random.random()
    if isinstance(a, AttributoTypeDateTime):
        return random_date(
            datetime.datetime(2022, 1, 1, 15, 0, 0, 0), datetime.datetime.now()
        )
    if isinstance(a, AttributoTypeList):
        elements_min = a.min_length if a.min_length is not None else 0
        elements_max = a.max_length if a.max_length is not None else 10
        elements_no = random.randrange(elements_min, elements_max + 1)
        return [_generate_attributo_value(a.sub_type, chemical_ids) for _ in range(elements_no)]  # type: ignore
    raise Exception(f"invalid attributo type {a}")


def _generate_attributi_map(
    attributi: list[DBAttributo], chemical_ids: list[int]
) -> AttributiMap:
    values: UntypedAttributiMap = {}
    for a in attributi:
        # in 20% of cases, leave attributo out of the equation
        if random.random() < 0.2:
            continue

        values[a.name] = _generate_attributo_value(a.attributo_type, [])

    return AttributiMap(
        types_dict={a.name: a for a in attributi},
        chemical_ids=chemical_ids,
        impl=values,
    )


def random_person_name() -> str:
    return random.choice(
        ["Henry", "Alessa", "Dominik", "Sasa", "Jerome", "Vivi", "Aida"]
    )


async def experiment_simulator_initialize_db(db: AsyncDB) -> None:
    async with db.begin() as conn:
        if await db.retrieve_chemicals(
            conn, await db.retrieve_attributi(conn, associated_table=None)
        ):
            return

        await db.create_attributo(
            conn,
            "producer",
            "Who produced the chemical?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            "compound",
            "What's in the chemical?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_CHEMICAL,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_PH,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeDecimal(range=NumericRange(0, True, None, False), suffix="pH"),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_FRAME_TIME,
            "",
            "kamzik",
            AssociatedTable.RUN,
            AttributoTypeDecimal(suffix="s", standard_unit=True),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_TARGET_FRAME_COUNT,
            "",
            "kamzik",
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_FLOW_RATE,
            "",
            "kamzik",
            AssociatedTable.RUN,
            AttributoTypeDecimal(
                range=NumericRange(0, True, None, False),
                suffix="ul/s",
                standard_unit=True,
            ),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_COMMENT,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_TRASH,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeBoolean(),
        )

        attributi = await db.retrieve_attributi(conn, associated_table=None)
        chemical_names: list[str] = []

        chemical_based_id = await db.create_experiment_type(
            conn,
            CHEMICAL_BASED,
            [AttributoNameAndRole(ATTRIBUTO_CHEMICAL, ChemicalType.CRYSTAL)],
        )
        time_resolved_id = await db.create_experiment_type(
            conn,
            TIME_RESOLVED,
            [
                AttributoNameAndRole(ATTRIBUTO_CHEMICAL, ChemicalType.CRYSTAL),
                AttributoNameAndRole(ATTRIBUTO_FLOW_RATE, ChemicalType.SOLUTION),
            ],
        )

        for _ in range(random.randrange(3, 10)):
            chemical_name = randomname.generate(
                "a/materials", ("n/plants", "n/food"), sep=" "
            )
            while chemical_name in chemical_names:
                chemical_name = generate("n/plants")
            chemical_id = await db.create_chemical(
                conn=conn,
                name=chemical_name,
                type_=ChemicalType.CRYSTAL,
                responsible_person="Rosalind Franklin",
                attributi=AttributiMap.from_types_and_json(
                    attributi,
                    chemical_ids=[],
                    raw_attributi={
                        "producer": random_person_name(),
                        "compound": randomname.generate("n/minerals"),
                    },
                ),
            )
            await db.create_data_set(
                conn,
                chemical_based_id,
                AttributiMap.from_types_and_json(
                    attributi, [chemical_id], {ATTRIBUTO_CHEMICAL: chemical_id}
                ),
            )
            for current_flow_rate in range(0, 4):
                await db.create_data_set(
                    conn,
                    time_resolved_id,
                    AttributiMap.from_types_and_json(
                        attributi,
                        [chemical_id],
                        {
                            ATTRIBUTO_CHEMICAL: chemical_id,
                            ATTRIBUTO_FLOW_RATE: current_flow_rate,
                        },
                    ),
                )


async def _start_run(
    db: AsyncDB,
    attributi: list[DBAttributo],
    chemical_ids: list[int],
    previous_run_id: int,
    previous_chemical: int | None,
) -> None:
    async with db.begin() as conn:

        chemical = (
            random.choice(chemical_ids)
            if previous_chemical is None or random.uniform(0, 100) > 80
            else previous_chemical
        )
        run_id = previous_run_id + 1
        await db.create_run(
            conn,
            run_id=run_id,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                chemical_ids=chemical_ids,
                raw_attributi={
                    ATTRIBUTO_STARTED: datetime.datetime.utcnow(),
                    ATTRIBUTO_FRAME_TIME: 1.0 / 130.0,
                    ATTRIBUTO_TRASH: random.uniform(0, 1) < 0.1,
                    ATTRIBUTO_PH: random.uniform(0, 14),
                    ATTRIBUTO_TARGET_FRAME_COUNT: 200_000,
                    ATTRIBUTO_FLOW_RATE: int(random.uniform(0, 5)),
                    ATTRIBUTO_COMMENT: random_gibberish()
                    if random.uniform(0, 100) > 70
                    else "",
                    ATTRIBUTO_CHEMICAL: chemical,
                },
            ),
            keep_manual_attributes_from_previous_run=(
                await db.retrieve_configuration(conn)
            ).auto_pilot,
        )
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=run_id,
                hits=int(random.uniform(1, 10000)),
                not_indexed_frames=int(random.uniform(1, 10000)),
                frames=int(random.uniform(1, 10000)),
                cell_description=parse_cell_description(
                    "tetragonal P c (79.2 79.2 38.0) (90 90 90)"
                ),
                point_group="4/mmm",
                chemical_id=1,
                runtime_status=DBIndexingResultRunning(
                    stream_file=Path("/home/homeless-shelter/dont-use.stream"),
                    job_id=1,
                    fom=_random_fom(None),
                ),
            ),
        )


def _random_fom(previous_fom: DBIndexingFOM | None) -> DBIndexingFOM:
    return DBIndexingFOM(
        hit_rate=random.uniform(0, 100),
        indexing_rate=random.uniform(0, 100),
        indexed_frames=previous_fom.indexed_frames + int(random.uniform(10, 100))
        if previous_fom is not None
        else 10,
    )


async def _stop_run(db: AsyncDB, run: DBRun) -> None:
    async with db.begin() as conn:
        previous_indexing_result = first(
            ir for ir in await db.retrieve_indexing_results(conn) if ir.run_id == run.id
        )
        if previous_indexing_result is not None and isinstance(
            previous_indexing_result.runtime_status, DBIndexingResultRunning
        ):
            rs = previous_indexing_result.runtime_status
            await db.update_indexing_result_status(
                conn,
                indexing_result_id=previous_indexing_result.id,
                runtime_status=DBIndexingResultDone(
                    stream_file=rs.stream_file, job_error=None, fom=rs.fom
                ),
            )
        new_attributi = run.attributi.copy()
        new_attributi.append_single(ATTRIBUTO_STOPPED, datetime.datetime.utcnow())
        await db.update_run_attributi(conn, run.id, new_attributi)


def _minutes_since(d: datetime.datetime) -> int:
    return int((datetime.datetime.utcnow() - d).total_seconds() / 60)


def random_gibberish() -> str:
    return "Test sentence please ignore"


async def _update_run(db: AsyncDB, run: DBRun, log: BoundLogger) -> None:
    log.info("Letting run go on for a bit")

    async with db.begin() as conn:
        ir = first(
            ir for ir in await db.retrieve_indexing_results(conn) if ir.run_id == run.id
        )
        if ir is None:
            return
        rs = ir.runtime_status
        if not isinstance(rs, DBIndexingResultRunning):
            return
        await db.update_indexing_result_status(
            conn,
            indexing_result_id=ir.id,
            runtime_status=DBIndexingResultRunning(
                stream_file=rs.stream_file, job_id=rs.job_id, fom=_random_fom(rs.fom)
            ),
        )


async def experiment_simulator_main_loop(db: AsyncDB, delay_seconds: float) -> None:
    logger.info("starting experiment simulator loop")
    while True:
        async with db.read_only_connection() as conn:
            attributi = await db.retrieve_attributi(conn, associated_table=None)
            chemical_ids = [s.id for s in await db.retrieve_chemicals(conn, attributi)]
            runs = await db.retrieve_runs(conn, attributi)

        last_run = safe_max(runs, key=lambda r: r.id)

        if last_run is None:
            logger.info("No runs, starting a new run")
            await _start_run(
                db,
                attributi=attributi,
                chemical_ids=chemical_ids,
                previous_run_id=0,
                previous_chemical=None,
            )
        else:
            log = logger.bind(run_id=last_run.id)

            stopped_time = last_run.attributi.select_datetime(ATTRIBUTO_STOPPED)

            if stopped_time is None:
                started = last_run.attributi.select_datetime_unsafe(ATTRIBUTO_STARTED)

                if _minutes_since(started) > random.uniform(5, 10):
                    log.info("Stopping run")
                    await _stop_run(db, last_run)
                else:
                    await _update_run(db, last_run, log)
            else:
                if _minutes_since(stopped_time) > random.uniform(0, 2):
                    log.info("Starting new run", run_id=last_run.id + 1)
                    await _start_run(
                        db,
                        attributi=attributi,
                        chemical_ids=chemical_ids,
                        previous_run_id=last_run.id,
                        previous_chemical=last_run.attributi.select_int_unsafe(
                            ATTRIBUTO_CHEMICAL
                        ),
                    )
                else:
                    log.info("Letting run wait for a bit")

        if random.uniform(0, 1000) > 990:
            logger.info("adding a new event")
            async with db.begin() as conn:
                await db.create_event(
                    conn, EventLogLevel.INFO, random_person_name(), random_gibberish()
                )

        await asyncio.sleep(delay_seconds)
