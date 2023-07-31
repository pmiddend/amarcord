import datetime
from io import StringIO

import structlog

from amarcord.amici.om.indexing_daemon import parse_stream
from amarcord.amici.om.indexing_daemon import update_runs_from_chunks
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.table_classes import DBRun

logger = structlog.stdlib.get_logger(__name__)


def test_parse_stream() -> None:
    lines = (
        "some crap lines",
        "----- Begin chunk -----",
        "other crap lines",
        "header/float/timestamp = 1657384967.878004",
        "more crap",
        "--- Begin crystal",
        "crystal crap",
        "--- End crystal",
        "----- End chunk -----",
    )
    chunks = list(parse_stream(StringIO("\n".join(lines))))
    assert len(chunks) == 1
    assert chunks[0][0] == 1
    assert chunks[0][1] == datetime.datetime(
        year=2022, month=7, day=9, hour=16, minute=42, second=47, microsecond=878004
    )


def test_update_runs() -> None:
    base_time = datetime.datetime(
        year=2022, month=7, day=9, hour=16, minute=42, second=47, microsecond=878004
    )
    # Simple test: we have one chunk with one crystal that has a time after the single run we have started.
    # We also have one chunk for this run without a crystal
    # Also, one chunk that doesn't match the run's time
    run_id = 1
    new_runs = update_runs_from_chunks(
        logger,
        [
            (1, base_time),
            (0, base_time),
            (0, base_time - datetime.timedelta(seconds=20)),
        ],
        [
            DBRun(
                id=run_id,
                files=[],
                attributi=AttributiMap.from_types_and_raw(
                    [
                        DBAttributo(
                            ATTRIBUTO_STARTED,
                            description="",
                            group="",
                            associated_table=AssociatedTable.RUN,
                            attributo_type=AttributoTypeDateTime(),
                        )
                    ],
                    [],
                    {ATTRIBUTO_STARTED: base_time - datetime.timedelta(seconds=50)},
                ),
            )
        ],
    )
    assert run_id in new_runs
    assert new_runs[run_id].indexed_crystals == 1
    assert new_runs[run_id].indexed_frames == 1
    assert new_runs[run_id].not_indexed_frames == 2
