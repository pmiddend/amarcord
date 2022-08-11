import pytest

from amarcord.cli.indexing_daemon import parse_stream  # type: ignore
from amarcord.cli.indexing_daemon import update_runs  # type: ignore


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
    chunks = list(parse_stream(lines))
    assert len(chunks) == 1
    assert chunks[0][0] == 1
    assert chunks[0][1] == pytest.approx(1657384967, 0.1)


def test_update_runs() -> None:
    base_time = 1657384967000.0
    # Simple test: we have one chunk with one crystal that has a time after the single run we have started.
    # We also have one chunk for this run without a crystal
    # Also, one chunk that doesn't match the run's time
    run_id = 1
    new_runs = update_runs(
        [(1, base_time), (0, base_time), (0, base_time - 20000)],
        [{"id": run_id, "attributi": {"started": base_time - 10000}}],
    )
    assert run_id in new_runs
    # pylint: disable=dict-direct-access
    assert new_runs[run_id]["indexed_crystals"] == 1
    # pylint: disable=dict-direct-access
    assert new_runs[run_id]["indexed_frames"] == 1
    # pylint: disable=dict-direct-access
    assert new_runs[run_id]["not_indexed_frames"] == 1
    # pylint: disable=dict-direct-access
    assert new_runs[run_id]["indexing_rate"] == pytest.approx(50.0, 0.1)
