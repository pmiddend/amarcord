import asyncio
import datetime
import logging
import random
from pathlib import Path
from typing import List, Optional

import randomname
from essential_generators import DocumentGenerator
from randomname import generate
from tap import Tap

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import (
    AsyncDB,
    create_ground_state_attributi,
    ATTRIBUTO_GROUP_MANUAL,
)
from amarcord.db.attributi import (
    ATTRIBUTO_STARTED,
    ATTRIBUTO_STOPPED,
)
from amarcord.db.attributi_map import AttributiMap, UntypedAttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoTypeInt,
    AttributoTypeChoice,
    AttributoTypeSample,
    AttributoTypeString,
    AttributoTypeBoolean,
    AttributoTypeDecimal,
    AttributoTypeDateTime,
    AttributoTypeList,
    AttributoType,
)
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import Connection
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.table_classes import DBRun, DBFileBlueprint
from amarcord.numeric_range import NumericRange
from amarcord.util import safe_max

TIME_RESOLVED = AttributoId("time-resolved")

SAMPLE_BASED = AttributoId("sample-based")

ATTRIBUTO_TRASH = AttributoId("trash")

ATTRIBUTO_COMMENT = AttributoId("comment")

ATTRIBUTO_FLOW_RATE = AttributoId("flow_rate")

ATTRIBUTO_PH = AttributoId("pH")

ATTRIBUTO_SAMPLE = AttributoId("sample")

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


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
    a: AttributoType, sample_ids: List[int]
) -> AttributoValue:
    if isinstance(a, AttributoTypeInt):
        return random.randint(-300, 300)
    if isinstance(a, AttributoTypeChoice):
        return random.choice(a.values)
    if isinstance(a, AttributoTypeSample):
        return random.choice(sample_ids)
    if isinstance(a, AttributoTypeString):
        return generate("n/*")
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
        result: List[AttributoValue] = []
        for _ in range(elements_no):
            result.append(_generate_attributo_value(a.sub_type, sample_ids))
        return result  # type: ignore
    raise Exception(f"invalid attributo type {a}")


def _generate_attributi_map(
    attributi: List[DBAttributo], sample_ids: List[int]
) -> AttributiMap:
    values: UntypedAttributiMap = {}
    for a in attributi:
        # in 20% of cases, leave attributo out of the equation
        if random.random() < 0.2:
            continue

        values[a.name] = _generate_attributo_value(a.attributo_type, [])

    return AttributiMap(
        types_dict={a.name: a for a in attributi}, sample_ids=sample_ids, impl=values
    )


def random_person_name() -> str:
    return random.choice(
        ["Henry", "Alessa", "Dominik", "Sasa", "Jerome", "Vivi", "Aida"]
    )


async def experiment_simulator_initialize_db(db: AsyncDB) -> None:
    async with db.begin() as conn:
        if await db.retrieve_attributi(conn, associated_table=None):
            return

        await create_ground_state_attributi(db, conn)

        await db.create_attributo(
            conn,
            "producer",
            "Who produced the sample?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.SAMPLE,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            "compound",
            "What's in the sample?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.SAMPLE,
            AttributoTypeString(),
        )
        await db.create_attributo(
            conn,
            ATTRIBUTO_SAMPLE,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeSample(),
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
        sample_names: List[str] = []

        await db.create_experiment_type(conn, SAMPLE_BASED, [ATTRIBUTO_SAMPLE])
        await db.create_experiment_type(
            conn, TIME_RESOLVED, [ATTRIBUTO_SAMPLE, ATTRIBUTO_FLOW_RATE]
        )

        for _ in range(random.randrange(3, 10)):
            sample_name = randomname.generate(
                "a/materials", ("n/plants", "n/food"), sep=" "
            )
            while sample_name in sample_names:
                sample_name = generate("n/plants")
            sample_id = await db.create_sample(
                conn,
                sample_name,
                AttributiMap.from_types_and_json(
                    attributi,
                    sample_ids=[],
                    raw_attributi={
                        "producer": random_person_name(),
                        "compound": randomname.generate("n/minerals"),
                    },
                ),
            )
            await db.create_data_set(
                conn,
                SAMPLE_BASED,
                AttributiMap.from_types_and_json(
                    attributi, [sample_id], {ATTRIBUTO_SAMPLE: sample_id}
                ),
            )
            for current_flow_rate in range(0, 4):
                await db.create_data_set(
                    conn,
                    TIME_RESOLVED,
                    AttributiMap.from_types_and_json(
                        attributi,
                        [sample_id],
                        {
                            ATTRIBUTO_SAMPLE: sample_id,
                            ATTRIBUTO_FLOW_RATE: current_flow_rate,
                        },
                    ),
                )


async def _start_run(
    db: AsyncDB,
    attributi: List[DBAttributo],
    sample_ids: List[int],
    previous_run_id: int,
    previous_sample: Optional[int],
) -> None:
    async with db.begin() as conn:
        sample = (
            random.choice(sample_ids)
            if previous_sample is None or random.uniform(0, 100) > 80
            else previous_sample
        )
        await db.create_run(
            conn,
            run_id=previous_run_id + 1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                sample_ids=sample_ids,
                raw_attributi={
                    ATTRIBUTO_STARTED: datetime.datetime.utcnow(),
                    ATTRIBUTO_TRASH: random.uniform(0, 1) < 0.1,
                    ATTRIBUTO_PH: random.uniform(0, 14),
                    ATTRIBUTO_FLOW_RATE: int(random.uniform(0, 5)),
                    ATTRIBUTO_COMMENT: random_gibberish()
                    if random.uniform(0, 100) > 70
                    else "",
                    ATTRIBUTO_SAMPLE: sample,
                },
            ),
            keep_manual_attributes_from_previous_run=False,
        )


async def _stop_run(db: AsyncDB, run: DBRun) -> None:
    async with db.begin() as conn:
        new_attributi = run.attributi.copy()
        new_attributi.append_single(ATTRIBUTO_STOPPED, datetime.datetime.utcnow())
        await db.update_run_attributi(conn, run.id, new_attributi)


def _minutes_since(d: datetime.datetime) -> int:
    return int((datetime.datetime.utcnow() - d).total_seconds() / 60)


gen = DocumentGenerator()


def random_gibberish() -> str:
    return gen.sentence()


async def _generate_cfel_results(
    db: AsyncDB, files_dir: Optional[Path], conn: Connection
) -> None:
    data_sets = await db.retrieve_data_sets(
        conn,
        await db.retrieve_sample_ids(conn),
        await db.retrieve_attributi(conn, associated_table=None),
    )

    await db.clear_cfel_analysis_results(conn)

    files = (
        random.choices(
            [DBFileBlueprint(f.name, f) for f in files_dir.iterdir()],
            k=random.randint(0, 3),
        )
        if files_dir is not None
        else []
    )

    for data_set in data_sets:
        await db.create_cfel_analysis_result(
            conn,
            DBCFELAnalysisResult(
                id=None,
                directory_name="/gpfs/cfel/foo/bar/baz",
                data_set_id=data_set.id,
                resolution=f"{random.uniform(1, 5):.2f}A",
                rsplit=random.uniform(1, 2),
                cchalf=random.uniform(1, 2),
                ccstar=random.uniform(1, 2),
                snr=random.uniform(1, 2),
                completeness=random.uniform(1, 2),
                multiplicity=random.uniform(1, 2),
                total_measurements=int(random.uniform(1, 30000)),
                unique_reflections=int(random.uniform(1, 30000)),
                num_patterns=int(random.uniform(1, 30000)),
                num_hits=int(random.uniform(1, 30000)),
                indexed_patterns=int(random.uniform(1, 30000)),
                indexed_crystals=int(random.uniform(1, 30000)),
                crystfel_version="0.9.1",
                ccstar_rsplit=random.uniform(-1000, 1000),
                created=datetime.datetime.utcnow(),
                files=[],
            ),
            files,
        )


async def experiment_simulator_main_loop(
    db: AsyncDB, files_dir: Optional[Path], delay_seconds: float
) -> None:
    logger.info("starting experiment simulator loop")
    while True:
        async with db.connect() as conn:
            attributi = await db.retrieve_attributi(conn, associated_table=None)
            sample_ids = [s.id for s in await db.retrieve_samples(conn, attributi)]
            runs = await db.retrieve_runs(conn, attributi)

        last_run = safe_max(runs, key=lambda r: r.id)

        if last_run is None:
            logger.info("No runs, starting a new run")
            await _start_run(
                db,
                attributi=attributi,
                sample_ids=sample_ids,
                previous_run_id=0,
                previous_sample=None,
            )
        else:
            stopped_time = last_run.attributi.select_datetime(ATTRIBUTO_STOPPED)

            if stopped_time is None:
                started = last_run.attributi.select_datetime_unsafe(ATTRIBUTO_STARTED)

                if _minutes_since(started) > random.uniform(5, 10):
                    logger.info(f"Stopping run {last_run.id}")
                    await _stop_run(db, last_run)
                else:
                    logger.info(f"Letting run {last_run.id} go on for a bit")
            else:
                if _minutes_since(stopped_time) > random.uniform(0, 2):
                    logger.info(f"Starting new run {last_run.id+1}")
                    await _start_run(
                        db,
                        attributi=attributi,
                        sample_ids=sample_ids,
                        previous_run_id=last_run.id,
                        previous_sample=last_run.attributi.select_int_unsafe(
                            ATTRIBUTO_SAMPLE
                        ),
                    )
                else:
                    logger.info(f"Letting run {last_run.id} wait for a bit")

        if random.uniform(0, 1000) > 990:
            logger.info("adding a new event")
            async with db.begin() as conn:
                await db.create_event(
                    conn, EventLogLevel.INFO, random_person_name(), random_gibberish()
                )

        async with db.begin() as conn:
            await _generate_cfel_results(db, files_dir, conn)

        await asyncio.sleep(delay_seconds)
