import asyncio
import datetime
import logging
import random
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import cast

import randomname
import structlog
from randomname import generate
from structlog.stdlib import BoundLogger
from tap import Tap

from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_name_and_role import AttributoIdAndRole
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
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultStatistic
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.table_classes import BeamtimeInput
from amarcord.db.table_classes import DBRunOutput
from amarcord.numeric_range import NumericRange
from amarcord.util import first
from amarcord.util import safe_max

TIME_RESOLVED = "time-resolved"

CHEMICAL_BASED = "chemical-based"

ATTRIBUTO_TRASH = "trash"

ATTRIBUTO_PRODUCER = "producer"
ATTRIBUTO_COMPOUND = "compound"

ATTRIBUTO_FLOW_RATE = "flow_rate"

ATTRIBUTO_FRAME_TIME = "frame_time"

ATTRIBUTO_TARGET_FRAME_COUNT = "target_frame_count"

ATTRIBUTO_CHEMICAL = "chemical"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = structlog.get_logger(__name__)


@dataclass(frozen=True)
class SimulatorData:
    beamtime_id: BeamtimeId
    attributo_producer: AttributoId
    attributo_compound: AttributoId
    attributo_chemical: AttributoId
    attributo_frame_time: AttributoId
    attributo_target_frame_count: AttributoId
    attributo_flow_rate: AttributoId
    attributo_trash: AttributoId


class Arguments(Tap):
    db_connection_url: (
        str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    )
    verbose: bool = False  # Show more log messages
    wait_time_seconds: (
        float  # How long to wait before simulating another event (in seconds)
    )


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
        return generate("n/*")  # type: ignore[no-any-return]
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
    assert isinstance(a, AttributoTypeList)
    elements_min = a.min_length if a.min_length is not None else 0
    elements_max = a.max_length if a.max_length is not None else 10
    elements_no = random.randrange(elements_min, elements_max + 1)
    return [_generate_attributo_value(a.sub_type, chemical_ids) for _ in range(elements_no)]  # type: ignore


def random_person_name() -> str:
    return random.choice(
        ["Henry", "Alessa", "Dominik", "Sasa", "Jerome", "Vivi", "Aida"]
    )


async def experiment_simulator_initialize_db(db: AsyncDB) -> SimulatorData:
    async with db.begin() as conn:
        beamtimes = await db.retrieve_beamtimes(conn)

        if beamtimes:
            attributi_by_name: dict[str, AttributoId] = {
                a.name: a.id
                for a in await db.retrieve_attributi(
                    conn, beamtimes[0].id, associated_table=None
                )
            }

            return SimulatorData(
                beamtime_id=beamtimes[0].id,
                attributo_producer=attributi_by_name[ATTRIBUTO_PRODUCER],
                attributo_compound=attributi_by_name[ATTRIBUTO_COMPOUND],
                attributo_chemical=attributi_by_name[ATTRIBUTO_CHEMICAL],
                attributo_frame_time=attributi_by_name[ATTRIBUTO_FRAME_TIME],
                attributo_target_frame_count=attributi_by_name[
                    ATTRIBUTO_TARGET_FRAME_COUNT
                ],
                attributo_flow_rate=attributi_by_name[ATTRIBUTO_FLOW_RATE],
                attributo_trash=attributi_by_name[ATTRIBUTO_TRASH],
            )

        beamtime_id = await db.create_beamtime(
            conn,
            BeamtimeInput(
                external_id="",
                proposal="",
                beamline="",
                title="",
                comment="",
                start=datetime.datetime.utcnow(),
                end=datetime.datetime.utcnow(),
            ),
        )

        attributo_producer = await db.create_attributo(
            conn,
            ATTRIBUTO_PRODUCER,
            beamtime_id,
            "Who produced the chemical?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        attributo_compound = await db.create_attributo(
            conn,
            ATTRIBUTO_COMPOUND,
            beamtime_id,
            "What's in the chemical?",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        attributo_chemical = await db.create_attributo(
            conn,
            ATTRIBUTO_CHEMICAL,
            beamtime_id,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )
        attributo_frame_time = await db.create_attributo(
            conn,
            ATTRIBUTO_FRAME_TIME,
            beamtime_id,
            "",
            "automatic",
            AssociatedTable.RUN,
            AttributoTypeDecimal(suffix="s", standard_unit=True),
        )
        attributo_target_frame_count = await db.create_attributo(
            conn,
            ATTRIBUTO_TARGET_FRAME_COUNT,
            beamtime_id,
            "",
            "automatic",
            AssociatedTable.RUN,
            AttributoTypeInt(),
        )
        attributo_flow_rate = await db.create_attributo(
            conn,
            ATTRIBUTO_FLOW_RATE,
            beamtime_id,
            "",
            "automatic",
            AssociatedTable.RUN,
            AttributoTypeDecimal(
                range=NumericRange(0, True, None, False),
                suffix="ul/s",
                standard_unit=True,
            ),
        )
        attributo_trash = await db.create_attributo(
            conn,
            ATTRIBUTO_TRASH,
            beamtime_id,
            "",
            ATTRIBUTO_GROUP_MANUAL,
            AssociatedTable.RUN,
            AttributoTypeBoolean(),
        )

        attributi = await db.retrieve_attributi(
            conn, beamtime_id, associated_table=None
        )
        chemical_names: list[str] = []

        chemical_based_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            CHEMICAL_BASED,
            [AttributoIdAndRole(attributo_chemical, ChemicalType.CRYSTAL)],
        )
        time_resolved_id = await db.create_experiment_type(
            conn,
            beamtime_id,
            TIME_RESOLVED,
            [
                AttributoIdAndRole(attributo_chemical, ChemicalType.CRYSTAL),
                AttributoIdAndRole(attributo_flow_rate, ChemicalType.SOLUTION),
            ],
        )

        config = await db.retrieve_configuration(conn, beamtime_id)
        await db.update_configuration(
            conn,
            beamtime_id,
            replace(config, current_experiment_type_id=chemical_based_id),
        )

        for _ in range(random.randrange(3, 10)):
            chemical_name = randomname.generate(
                "a/materials", ("n/plants", "n/food"), sep=" "
            )
            while chemical_name in chemical_names:
                chemical_name = generate("n/plants")
            chemical_id = await db.create_chemical(
                conn=conn,
                beamtime_id=beamtime_id,
                name=chemical_name,
                type_=ChemicalType.CRYSTAL,
                responsible_person="Rosalind Franklin",
                attributi=AttributiMap.from_types_and_json_dict(
                    attributi,
                    json_dict={
                        str(attributo_producer): random_person_name(),
                        str(attributo_compound): randomname.generate("n/minerals"),
                    },
                ),
            )
            await db.create_data_set(
                conn,
                beamtime_id,
                chemical_based_id,
                AttributiMap.from_types_and_json_dict(
                    attributi, {str(attributo_chemical): chemical_id}
                ),
            )
            for current_flow_rate in range(0, 4):
                await db.create_data_set(
                    conn,
                    beamtime_id,
                    time_resolved_id,
                    AttributiMap.from_types_and_json_dict(
                        attributi,
                        {
                            str(attributo_chemical): chemical_id,
                            str(attributo_flow_rate): current_flow_rate,
                        },
                    ),
                )
    return SimulatorData(
        beamtime_id=beamtime_id,
        attributo_producer=attributo_producer,
        attributo_compound=attributo_compound,
        attributo_chemical=attributo_chemical,
        attributo_frame_time=attributo_frame_time,
        attributo_target_frame_count=attributo_target_frame_count,
        attributo_flow_rate=attributo_flow_rate,
        attributo_trash=attributo_trash,
    )


async def _start_run(
    db: AsyncDB,
    simulator_data: SimulatorData,
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
        run_id = RunExternalId(previous_run_id + 1)
        beamtime_id = simulator_data.beamtime_id
        run_internal_id = await db.create_run(
            conn,
            run_external_id=run_id,
            beamtime_id=simulator_data.beamtime_id,
            started=datetime.datetime.utcnow(),
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi,
                raw_attributi={
                    simulator_data.attributo_frame_time: 1.0 / 130.0,
                    simulator_data.attributo_trash: random.uniform(0, 1) < 0.1,
                    simulator_data.attributo_target_frame_count: 200_000,
                    simulator_data.attributo_flow_rate: int(random.uniform(0, 5)),
                    simulator_data.attributo_chemical: chemical,
                },
            ),
            experiment_type_id=cast(
                int,
                (
                    await db.retrieve_configuration(conn, beamtime_id)
                ).current_experiment_type_id,
            ),
            keep_manual_attributes_from_previous_run=(
                await db.retrieve_configuration(conn, beamtime_id)
            ).auto_pilot,
        )
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                run_id=run_internal_id,
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
        indexed_frames=(
            previous_fom.indexed_frames + int(random.uniform(10, 100))
            if previous_fom is not None
            else 10
        ),
        detector_shift_x_mm=random.uniform(0, 10.0),
        detector_shift_y_mm=random.uniform(0, 10.0),
    )


async def _stop_run(db: AsyncDB, run: DBRunOutput) -> None:
    async with db.begin() as conn:
        previous_indexing_result = first(
            ir
            for ir in await db.retrieve_indexing_results(conn, beamtime_id=None)
            if ir.run_id == run.id
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
        await db.update_run(
            conn,
            internal_id=run.id,
            stopped=datetime.datetime.utcnow(),
            attributi=new_attributi,
            new_experiment_type_id=None,
        )


def _minutes_since(d: datetime.datetime) -> int:
    return int((datetime.datetime.utcnow() - d).total_seconds() / 60)


def random_gibberish() -> str:
    return "Test sentence please ignore"


async def _update_run(db: AsyncDB, run: DBRunOutput, log: BoundLogger) -> None:
    log.info("Letting run go on for a bit")

    async with db.begin() as conn:
        ir = first(
            ir
            for ir in await db.retrieve_indexing_results(
                conn, beamtime_id=run.beamtime_id
            )
            if ir.run_id == run.id
        )
        if ir is None:
            return
        rs = ir.runtime_status
        if not isinstance(rs, DBIndexingResultRunning):
            return
        log.info("Updating indexing result")
        await db.update_indexing_result_status(
            conn,
            indexing_result_id=ir.id,
            runtime_status=DBIndexingResultRunning(
                stream_file=rs.stream_file, job_id=rs.job_id, fom=_random_fom(rs.fom)
            ),
        )
        all_stats = await db.retrieve_indexing_result_statistics(
            conn, run.beamtime_id, ir.id
        )
        stats = (
            all_stats[-1]
            if all_stats
            else DBIndexingResultStatistic(
                indexing_result_id=ir.id,
                time=datetime.datetime.utcnow(),
                frames=0,
                hits=0,
                indexed_frames=0,
                indexed_crystals=0,
            )
        )
        new_frames = int(random.uniform(0, 100))
        frames = stats.frames + new_frames
        new_hits = int(new_frames * random.uniform(0, 0.1))
        hits = stats.hits + new_hits
        indexed_frames = int(stats.indexed_frames + new_hits * random.uniform(0, 1))
        await db.add_indexing_result_statistic(
            conn,
            s=DBIndexingResultStatistic(
                indexing_result_id=ir.id,
                time=datetime.datetime.utcnow(),
                frames=frames,
                hits=hits,
                indexed_frames=indexed_frames,
                indexed_crystals=indexed_frames,
            ),
        )


async def experiment_simulator_main_loop(
    db: AsyncDB, simulator_data: SimulatorData, delay_seconds: float
) -> None:
    logger.info("starting experiment simulator loop")
    while True:
        async with db.begin() as conn:
            attributi = await db.retrieve_attributi(
                conn, simulator_data.beamtime_id, associated_table=None
            )
            chemical_ids = [
                s.id
                for s in await db.retrieve_chemicals(
                    conn, simulator_data.beamtime_id, attributi
                )
            ]
            runs = await db.retrieve_runs(conn, simulator_data.beamtime_id, attributi)

        last_run = safe_max(runs, key=lambda r: r.id)

        if last_run is None:
            logger.info("No runs, starting a new run")
            await _start_run(
                db,
                simulator_data,
                attributi=attributi,
                chemical_ids=chemical_ids,
                previous_run_id=0,
                previous_chemical=None,
            )
        else:
            log = logger.bind(run_id=last_run.id)

            stopped_time = last_run.stopped

            if stopped_time is None:
                started = last_run.started

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
                        simulator_data,
                        attributi=attributi,
                        chemical_ids=chemical_ids,
                        previous_run_id=last_run.id,
                        previous_chemical=last_run.attributi.select_int_unsafe(
                            simulator_data.attributo_chemical
                        ),
                    )
                else:
                    log.info("Letting run wait for a bit")

        if random.uniform(0, 1000) > 990:
            logger.info("adding a new event")
            async with db.begin() as conn:
                await db.create_event(
                    conn,
                    simulator_data.beamtime_id,
                    EventLogLevel.INFO,
                    random_person_name(),
                    random_gibberish(),
                )

        await asyncio.sleep(delay_seconds)
