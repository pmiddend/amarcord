# type: ignore
import argparse
import json
import logging
import sys
from time import time
from urllib.request import Request
from urllib.request import urlopen

NOT_INDEXED_FRAMES = "not_indexed_frames"
INDEXED_CRYSTALS = "indexed_crystals"
INDEXED_FRAMES = "indexed_frames"
INDEXING_RATE = "indexing_rate"
SECONDS_TO_BATCH = 10.0

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="Continuously read CrystFEL stream from stdin, set indexing rate"
)
parser.add_argument(
    "--rest-url",
    required=True,
    help="URL of the REST database server to send the results to",
)


def _create_attributo(url, name, type_):
    with urlopen(
        Request(
            f"{url}/api/attributi",
            method="POST",
            data=json.dumps(
                {
                    "name": name,
                    "group": "om",
                    "description": "",
                    "associatedTable": "run",
                    "type": type_,
                }
            ).encode("utf-8"),
        )
    ):
        pass


def _create_attributi(url):
    existing_attributi = {
        a.get("name") for a in _retrieve_attributi(url).get("attributi", [])
    }

    if INDEXING_RATE not in existing_attributi:
        _create_attributo(url, INDEXING_RATE, {"type": "number", "suffix": "%"})
    if INDEXED_FRAMES not in existing_attributi:
        _create_attributo(url, INDEXED_FRAMES, {"type": "integer"})
    if NOT_INDEXED_FRAMES not in existing_attributi:
        _create_attributo(url, NOT_INDEXED_FRAMES, {"type": "integer"})
    if INDEXED_CRYSTALS not in existing_attributi:
        _create_attributo(url, INDEXED_CRYSTALS, {"type": "integer"})


def _retrieve_attributi(url):
    with urlopen(Request(f"{url}/api/attributi", method="GET")) as conn:
        return json.loads(conn.read().decode("utf-8"))


def _retrieve_latest_run(url):
    with urlopen(Request(f"{url}/api/runs/latest", method="GET")) as conn:
        return json.loads(conn.read().decode("utf-8"))


def _retrieve_runs(url):
    with urlopen(Request(f"{url}/api/runs", method="GET")) as conn:
        return json.loads(conn.read().decode("utf-8"))


def _update_run(url, run_id, attributi):
    with urlopen(
        Request(
            f"{url}/api/runs",
            method="PATCH",
            data=json.dumps(
                {
                    "id": run_id,
                    "attributi": attributi,
                }
            ).encode("utf-8"),
        )
    ):
        pass


def parse_stream(stream):
    crystals_in_chunk = 0
    chunk_timestamp = None
    line_no = 1
    timestamp_prefix = "header/float/timestamp = "
    for line in stream:
        if line.startswith("----- Begin chunk"):
            crystals_in_chunk = 0
        elif line.startswith("----- End chunk"):
            if chunk_timestamp is None:
                logger.info("chunk without timestamp received")
            else:
                yield crystals_in_chunk, chunk_timestamp
        elif line.startswith("--- Begin crystal"):
            crystals_in_chunk += 1
        elif line.startswith(timestamp_prefix):
            chunk_timestamp = float(line[len(timestamp_prefix) :])
        line_no += 1


def update_runs(chunks, runs):
    if not chunks:
        return {}
    new_run_data = {}
    for crystals_in_chunk, chunk_timestamp in chunks:
        for run in runs:
            attributi = run.get("attributi", {})
            started = attributi.get("started")
            stopped = attributi.get("stopped")
            run_id = run.get("id")
            assert run_id is not None, f"run has no run id: {run}"

            logger.info(
                f"chunk timestamp: {chunk_timestamp}, started: {started}, stopped: {stopped}"
            )
            if (
                chunk_timestamp < started
                or stopped is not None
                and chunk_timestamp > stopped
            ):
                continue
            current_run_data = new_run_data.get(
                run_id,
                {
                    INDEXED_CRYSTALS: attributi.get(INDEXED_CRYSTALS, 0),
                    INDEXED_FRAMES: attributi.get(INDEXED_FRAMES, 0),
                    NOT_INDEXED_FRAMES: attributi.get(NOT_INDEXED_FRAMES, 0),
                },
            )
            indexed_frames = current_run_data.get(INDEXED_FRAMES, 0) + (
                1 if crystals_in_chunk else 0
            )
            not_indexed_frames = current_run_data.get(NOT_INDEXED_FRAMES, 0) + (
                0 if crystals_in_chunk else 1
            )
            total_frames = indexed_frames + not_indexed_frames
            new_run_data[run_id] = {
                INDEXED_CRYSTALS: current_run_data.get(INDEXED_CRYSTALS, 0)
                + crystals_in_chunk,
                INDEXED_FRAMES: indexed_frames,
                NOT_INDEXED_FRAMES: not_indexed_frames,
                INDEXING_RATE: indexed_frames / total_frames * 100.0
                if total_frames > 0
                else 0.0,
            }
            # We've found one run. That's enough.
            break
    logger.info(f"new run data: {new_run_data}")
    return new_run_data


def _update_runs_in_db(url, chunks):
    new_run_data = update_runs(chunks, _retrieve_runs(url).get("runs", None))
    for run_id, run_attributi in new_run_data.items():
        _update_run(url, run_id, run_attributi)


def main():
    args = parser.parse_args()

    _create_attributi(args.rest_url)

    last_request = time()
    chunks = []
    for this_crystals_in_chunk, this_chunk_timestamp in parse_stream(sys.stdin):
        logger.info(
            f"new chunk with {this_crystals_in_chunk} crystal(s), time {this_chunk_timestamp}"
        )
        # Chunk timestamp is in seconds, we store milliseconds
        chunks.append((this_crystals_in_chunk, this_chunk_timestamp * 1000.0))
        if time() - last_request < SECONDS_TO_BATCH:
            continue
        _update_runs_in_db(args.rest_url, chunks)
        chunks.clear()
    _update_runs_in_db(args.rest_url, chunks)


if __name__ == "__main__":
    main()
