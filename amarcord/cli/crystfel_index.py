#!/usr/bin/env python3

import getpass
import glob
import hashlib
import json
import logging
import os
import re
import shlex
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import urllib.error
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from functools import reduce
from itertools import islice
from pathlib import Path
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory
from time import sleep
from time import time
from typing import IO
from typing import Any
from typing import Callable
from typing import Final
from typing import Generator
from typing import Iterable
from typing import MutableSequence
from typing import NoReturn
from typing import TypeVar
from urllib import request

_SLURM_JOB_STATUS_RUNNING = (
    "PENDING",
    "RUNNING",
    "REQUEUED",
    "RESIZING",
    "SUSPENDED",
    "QUEUED",
    "COMPLETING",
)
_MASK_REGEX = re.compile(r"^mask([0-7])?_file\s*=\s*([^\n]+)$")
_DESY_TEMP_DIR = Path("/scratch")

# This is for parsing DESY's metadata JSON format in the case of online indexing
METADATA_KEY_ASAPO_ENDPOINT = "endpoint"

METADATA_KEY_ASAPO = "asapo"

METADATA_KEY_ASAPO_TOKEN_PATH = "beamtimeTokenPath"

METADATA_KEY_BEAMTIME_ID = "beamtimeId"
METADATA_KEY_CORE_PATH = "corePath"


def crystfel_geometry_retrieve_masks(geometry_file: Path) -> list[Path]:
    """Retrieve lines matching masks in a CrystFEL geometry file (used
    to calculate the hash not only for the CrystFEL geometry file
    itself, but also for its "dependent files" like the masks)"""
    masks: list[Path] = []
    with geometry_file.open("r", encoding="utf-8") as f:
        for line_unstripped in f:
            match = _MASK_REGEX.match(line_unstripped)
            if match is None:
                continue

            masks.append(Path(match.group(2)))
    return masks


def sha256_file(p: Path) -> str:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def sha256_combination(hashes: Iterable[bytes]) -> str:
    return hashlib.sha256(b"".join(hashes)).hexdigest()


def sha256_file_bytes(p: Path) -> bytes:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).digest()


def crystfel_geometry_hash(p: Path) -> str:
    return sha256_combination(
        [sha256_file_bytes(p)]
        + [sha256_file_bytes(mask) for mask in crystfel_geometry_retrieve_masks(p)]
    )


# We want to log to a list of lines, so we can then send it from the
# secondary to the primary job and output it there.
#
# Source:
# https://stackoverflow.com/questions/36408496/python-logging-handler-to-append-to-list
class ListHandler(logging.Handler):
    def __init__(self, log_list_: MutableSequence[str]) -> None:
        # run the regular Handler __init__
        logging.Handler.__init__(self)
        # Our custom argument
        self.log_list = log_list_

    def emit(self, record: logging.LogRecord) -> None:
        # record.message is the log message
        self.log_list.append(self.format(record).rstrip("\n"))


logger = logging.getLogger(__name__)
# Better to do it like this, but for now...
# log_list: Deque[str] = deque(maxlen=20)
# ...save the whole log
log_list: list[str] = []
logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(), ListHandler(log_list)],
)

T = TypeVar("T")

OFF_INDEX_ENVIRON_INPUT_FILE_GLOBS = "AMARCORD_INPUT_FILE_GLOBS"
OFF_INDEX_ENVIRON_SLURM_PARTITION_TO_USE = "AMARCORD_SLURM_PARTITION_TO_USE"
OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID = "AMARCORD_INDEXING_RESULT_ID"
OFF_INDEX_ENVIRON_AMARCORD_API_URL = "AMARCORD_API_URL"
OFF_INDEX_ENVIRON_JOB_STYLE = "AMARCORD_JOB_STYLE"
OFF_INDEX_ENVIRON_USE_SLURM = "AMARCORD_USE_SLURM"
OFF_INDEX_ENVIRON_JOB_STYLE_PRIMARY = "primary"
OFF_INDEX_ENVIRON_JOB_STYLE_SECONDARY = "secondary"
OFF_INDEX_ENVIRON_JOB_STYLE_ONLINE = "online"
OFF_INDEX_ENVIRON_SECONDARY_JOB_ARRAY_ID = "AMARCORD_JOB_ARRAY_ID"
OFF_INDEX_ENVIRON_STREAM_FILE = "AMARCORD_STREAM_FILE"
OFF_INDEX_ENVIRON_GEOMETRY_FILE = "AMARCORD_GEOMETRY_FILE"
OFF_INDEX_ENVIRON_SECONDARY_CELL_FILE = "AMARCORD_CELL_FILE"
OFF_INDEX_ENVIRON_CELL_DESCRIPTION = "AMARCORD_CELL_DESCRIPTION"
OFF_INDEX_ENVIRON_CRYSTFEL_PATH = "AMARCORD_CRYSTFEL_PATH"
OFF_INDEX_ENVIRON_GNUPLOT_PATH = "AMARCORD_GNUPLOT_PATH"
OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS = "AMARCORD_INDEXAMAJIG_PARAMS"
OFF_INDEX_ENVIRON_RUN_ID = "AMARCORD_RUN_ID"
OFF_INDEX_ENVIRON_PERFORMANCE_METRICS = "AMARCORD_PERFORMANCE_METRICS"
ON_INDEX_ENVIRON_ASAPO_SOURCE = "AMARCORD_ASAPO_SOURCE"
ON_INDEX_ENVIRON_AMARCORD_CPU_COUNT_MULTIPLIER = "AMARCORD_CPU_COUNT_MULTIPLIER"

# We should probably make this less of a constant
MAXWELL_URL: Final = "https://max-portal.desy.de/sapi/slurm/v0.0.38"
# No real reason why we haven't parametrized this into an environment
# argument (see the other options below). 1000 seems like a nice
# number for now, and CrystFEL uses the same:
#
# https://gitlab.desy.de/thomas.white/crystfel/-/blob/66a5efece6c2c219af873f1522dd4a82003bf6b9/src/gui_backend_slurm.c#L985
IMAGES_PER_JOB: Final = 1_000
# Same constant, 1000, to define one job array to have 1000 jobs (so there's IMAGES_PER_JOB*1000 images in a job array)
INDEXAMAJIG_JOBS_PER_JOB_ARRAY: Final = 1000

# For an extensive intro on this primary/secondary business, see the manual
JOB_STYLE: Final = os.environ.get(
    OFF_INDEX_ENVIRON_JOB_STYLE, OFF_INDEX_ENVIRON_JOB_STYLE_PRIMARY
)
_INDEXING_RE: Final = re.compile(
    r"(\d+) images processed, (\d+) hits \([^)]+\), (\d+) indexable \([^)]+\), (\d+) crystals.*"
)

# The FPS killer monitors the indexing frame rate and kills jobs if
# they're too slow. Set to zero to disable (statically, of course).
_FPS_KILLER_ONLINE_SECONDS = 5 * 60
_FPS_KILLER_OFFLINE: Final = False


@contextmanager
def set_directory(path: str):
    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


@dataclass(frozen=True, eq=True)
class CrystFELCellFile:
    lattice_type: str
    centering: str
    unique_axis: None | str

    a: float
    b: float
    c: float
    alpha: float
    beta: float
    gamma: float


_cell_description_regex = re.compile(
    r"(triclinic|monoclinic|orthorhombic|tetragonal|rhombohedral|hexagonal|cubic)\s+([PABCIFRH])\s+([abc?*])\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)\s+\(([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\s+([0-9]*(?:\.[0-9]*)?)\)"
)


def coparse_cell_description(s: CrystFELCellFile) -> str:
    ua = s.unique_axis if s.unique_axis is not None else "?"
    return f"{s.lattice_type} {s.centering} {ua} ({s.a} {s.b} {s.c}) ({s.alpha} {s.beta} {s.gamma})"


def parse_cell_description(s: str) -> None | CrystFELCellFile:
    match = _cell_description_regex.fullmatch(s)
    if match is None:
        return None
    unique_axis = match.group(3)
    return CrystFELCellFile(
        lattice_type=match.group(1),
        centering=match.group(2),
        unique_axis=None if unique_axis == "?" else unique_axis,
        a=float(match.group(4)),
        b=float(match.group(5)),
        c=float(match.group(6)),
        alpha=float(match.group(7)),
        beta=float(match.group(8)),
        gamma=float(match.group(9)),
    )


def coparse_cell_file(c: CrystFELCellFile, f: IO[str]) -> None:
    f.write("CrystFEL unit cell file version 1.0\n\n")
    f.write(f"lattice_type = {c.lattice_type}\n")
    f.write(f"centering = {c.centering}\n")
    if c.unique_axis is not None:
        f.write(f"unique_axis = {c.unique_axis}\n\n")
    f.write(f"a = {c.a} A\n")
    f.write(f"b = {c.b} A\n")
    f.write(f"c = {c.c} A\n")
    f.write(f"al = {c.alpha} deg\n")
    f.write(f"be = {c.beta} deg\n")
    f.write(f"ga = {c.gamma} deg\n")


def write_cell_file(c: CrystFELCellFile, p: Path) -> None:
    with p.open("w") as f:
        coparse_cell_file(c, f)


@dataclass(frozen=True)
class PrimaryArgs:
    use_slurm: bool
    # can be empty if slurm isn't used
    slurm_partition_to_use: str
    workload_manager_job_id: int
    # Where to store the stream file into
    stream_file: Path
    amarcord_api_url: None | str
    # the file list gets extremely long sometimes, so don't include this by default
    input_files: list[Path] = field(repr=False)
    cell_description: None | str
    # Can be empty, in which case we search dynamically for the geometry
    geometry_file: None | Path
    crystfel_path: Path
    amarcord_indexing_result_id: int
    # The primary job spawns sub-jobs, and needs information for Maxwell on this
    maxwell_headers: dict[str, str]
    indexamajig_params: str
    use_auto_geom_refinement: bool
    gnuplot_path: None | Path


@dataclass(frozen=True)
class SecondaryArgs:
    # If we're on SLURM, the secondary jobs need to access their own
    # job array task ID to store their result into the proper sqlite
    # DB cell
    use_slurm: bool
    amarcord_api_url: None | str
    amarcord_indexing_result_id: int
    cell_file: None | Path
    job_array_id: int
    crystfel_path: Path
    geometry_file: Path
    indexamajig_params: list[str]
    use_auto_geom_refinement: bool


@dataclass(frozen=True)
class OnlineArgs:
    workload_manager_job_id: int
    # Where to store the stream file into
    stream_file: Path
    amarcord_api_url: None | str
    asapo_source: str
    # To tweak the -j argument, still keeping it a bit dynamic (depending on the machine used)
    cpu_count_multiplier: None | float
    # Can be None, in which case we search for the geometry file dynamically
    geometry_file: None | Path
    cell_description: None | str
    amarcord_indexing_result_id: int
    crystfel_path: Path
    indexamajig_params: list[str]
    use_auto_geom_refinement: bool
    external_run_id: int
    gnuplot_path: None | Path


def write_error_json(args: PrimaryArgs | OnlineArgs, error: str) -> None:
    logger.info(
        f"contacting AMARCORD at {args.amarcord_api_url} to send erroneous status: {error}"
    )
    req = request.Request(
        f"{args.amarcord_api_url}/api/indexing/{args.amarcord_indexing_result_id}/finish-with-error",
        data=json.dumps(
            {
                "workload_manager_job_id": args.workload_manager_job_id,
                "error_message": error,
                "latest_log": "\n".join(log_list),
            },
            allow_nan=False,
            indent=2,
        ).encode("utf-8"),
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req) as response:
        logger.info(f"received the following response from server: {response.read()}")


@dataclass(frozen=True)
class IndexingFom:
    frames: int
    hits: int
    indexed_frames: int
    indexed_crystals: int
    detector_shift_x_mm: None | float = None
    detector_shift_y_mm: None | float = None
    # This is the "file id" in the AMARCORD DB for the unit cell file,
    # which we upload from this script and then transfer the ID in the
    # final result
    unit_cell_histograms_id: None | int = None


def add_indexing_fom(l: IndexingFom, r: IndexingFom) -> IndexingFom:
    return IndexingFom(
        frames=l.frames + r.frames,
        hits=l.hits + r.hits,
        indexed_frames=l.indexed_frames + r.indexed_frames,
        indexed_crystals=l.indexed_crystals + r.indexed_crystals,
    )


def exit_with_error(args: PrimaryArgs | OnlineArgs, error: str) -> NoReturn:
    write_error_json(args, error)
    sys.exit(1)


def write_status_still_running(
    args: PrimaryArgs | OnlineArgs, geometry_hash: str, line: IndexingFom
) -> None:
    if args.amarcord_api_url is None:
        return
    # Explicit dict annotation to force string value type (otherwise
    # Path might sneak in and cannot be serialized)
    status_dict: dict[str, str | int | float | None] = {
        "workload_manager_job_id": args.workload_manager_job_id,
        "stream_file": str(args.stream_file),
        "frames": line.frames,
        "hits": line.hits,
        "indexed_frames": line.indexed_frames,
        "indexed_crystals": line.indexed_crystals,
        "detector_shift_x_mm": line.detector_shift_x_mm,
        "detector_shift_y_mm": line.detector_shift_y_mm,
        "geometry_file": str(args.geometry_file) if args.geometry_file else None,
        "geometry_hash": geometry_hash,
        "latest_log": "\n".join(log_list),
    }
    request_url = f"{args.amarcord_api_url}/api/indexing/{args.amarcord_indexing_result_id}/still-running"

    # Necessary - otherwise we will have a recursion where we send what's being logged, and log it, and so on
    status_dict_without_log = dict(status_dict)
    status_dict_without_log.pop("latest_log")
    logger.info(
        f"contacting AMARCORD at {request_url} to send status: {status_dict_without_log}"
    )
    req = request.Request(
        request_url,
        data=json.dumps(
            status_dict,
            allow_nan=False,
            indent=2,
        ).encode("utf-8"),
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req) as response:
        logger.info(f"received the following response from server: {response.read()}")


def write_status_success(
    args: PrimaryArgs | OnlineArgs,
    program_version: str,
    geometry_hash: str,
    generated_geometry_file: str,
    line: IndexingFom,
) -> None:
    if args.amarcord_api_url is None:
        return
    request_url = f"{args.amarcord_api_url}/api/indexing/{args.amarcord_indexing_result_id}/success"
    # Explicit dict annotation to force string value type (otherwise
    # Path might sneak in and cannot be serialized)
    status_dict: dict[str, str | int | float | None] = {
        "workload_manager_job_id": args.workload_manager_job_id,
        "stream_file": str(args.stream_file),
        "program_version": program_version,
        "frames": line.frames,
        "hits": line.hits,
        "indexed_frames": line.indexed_frames,
        "indexed_crystals": line.indexed_crystals,
        "detector_shift_x_mm": line.detector_shift_x_mm,
        "detector_shift_y_mm": line.detector_shift_y_mm,
        "geometry_file": str(args.geometry_file) if args.geometry_file else None,
        "generated_geometry_file": generated_geometry_file,
        "geometry_hash": geometry_hash,
        "unit_cell_histograms_id": line.unit_cell_histograms_id,
        "latest_log": "\n".join(log_list),
    }
    # Necessary - otherwise we will have a recursion where we send what's being logged, and log it, and so on
    status_dict_without_log = dict(status_dict)
    status_dict_without_log.pop("latest_log")
    logger.info(
        f"contacting AMARCORD at {request_url} to send final status: {status_dict_without_log}"
    )
    req = request.Request(
        request_url,
        data=json.dumps(
            status_dict,
            allow_nan=False,
            indent=2,
        ).encode("utf-8"),
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req) as response:
        logger.info(f"received the following response from server: {response.read()}")


def db_execute_timed(
    db: sqlite3.Connection, statement: str, args: None | tuple[Any, ...]
) -> Any:
    before = time()
    if args is None:
        result = db.execute(statement)
    else:
        result = db.execute(statement, args)
    after = time()
    if OFF_INDEX_ENVIRON_PERFORMANCE_METRICS in os.environ:
        logger.info(f"query {statement}: {after - before}s")
    return result


def clean_intermediate_files(job_id: int) -> None:
    for fn in Path("./").glob("job-*-*-stdout.txt"):
        fn.unlink()
    for fn in Path("./").glob("job-*-*-stderr.txt"):
        fn.unlink()
    for fn in Path("./").glob("job-*.lst"):
        fn.unlink()
    try:
        for millepede_files_dir in Path(f"{job_id}-millepede-files").iterdir():
            for bin_file in millepede_files_dir.glob("*.bin"):
                bin_file.unlink()
            millepede_files_dir.rmdir()
    except:
        pass


# See
# https://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
def batched(iterable: Iterable[T], n: int) -> Generator[tuple[T, ...], None, None]:
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


class JobArrayLocal:
    def __init__(self, proc: subprocess.Popen[bytes]) -> None:
        self.proc = proc

    def wait(self) -> None | int:
        self.proc.poll()
        return self.proc.returncode


class JobArraySlurm:
    def __init__(self, job_id: int) -> None:
        self.job_id = job_id


JobArray = JobArrayLocal | JobArraySlurm


def cancel_job(args: PrimaryArgs, job_id: JobArray) -> None:
    if isinstance(job_id, JobArrayLocal):
        try:
            job_id.proc.kill()
        except:
            logger.warning("couldn't kill local job")
        return
    cancel_job_slurm(args, job_id)


def cancel_job_slurm(args: PrimaryArgs, job_id: JobArraySlurm) -> None:
    req = request.Request(
        f"{MAXWELL_URL}/job/{job_id.job_id}",
        method="DELETE",
        headers=args.maxwell_headers,
    )

    logger.info(f"sending cancel request for job {job_id.job_id}")
    try:
        with request.urlopen(req) as response:
            response_content_raw = response.read().decode("utf-8")
            logger.info(f"cancel request answer: {response_content_raw}")
    except:
        logger.exception("cancel request exception")


@contextmanager
def temp_fifo():
    """Context Manager for creating named pipes with temporary names."""
    tmpdir = tempfile.mkdtemp()
    filename = os.path.join(tmpdir, "fifo")  # Temporary filename
    os.mkfifo(filename)  # Create FIFO
    try:
        yield filename
    finally:
        os.unlink(filename)  # Remove file
        os.rmdir(tmpdir)  # Remove directory


def initialize_db(
    args: PrimaryArgs, geometry_hash: str, db: sqlite3.Connection
) -> set[int]:
    with NamedTemporaryFile() as input_files_file:
        for input_file in args.input_files:
            input_files_file.write((str(input_file) + "\n").encode("utf-8"))
        input_files_file.flush()
        with NamedTemporaryFile() as output_file:
            logger.info(f"=> listing events into {output_file}")

            list_events_args: list[str] = [
                f"{args.crystfel_path}/bin/list_events",
                "-i",
                input_files_file.name,
                "-g",
                str(args.geometry_file),
                "-o",
                output_file.name,
            ]
            logger.info("list_events args: " + " ".join(list_events_args))
            result = subprocess.run(list_events_args, check=True)
            logger.info(f"list events completed: {result}")
            # This used to be a FIFO, but due to a bug that wasn't
            # diagnosed completely, for now it's a regular file. Which
            # is a bummer, because with a FIFO you could do parallel
            # processing, but whatever.
            with open(output_file.name, "r", encoding="utf-8") as fifo_file_obj:
                logger.info(f"opened list files: {output_file}")
                job_array_id = 0
                indexamajig_job_id = 0
                job_array_ids: set[int] = {0}
                logger.info(
                    f"start adding indexamajig jobs to DB ({IMAGES_PER_JOB} images per job)"
                )
                for event_batch in batched(
                    (x.strip() for x in fifo_file_obj), IMAGES_PER_JOB
                ):
                    if indexamajig_job_id % 50 == 0:
                        logger.info(
                            f"{indexamajig_job_id} indexamajig jobs added so far..."
                        )
                        write_status_still_running(
                            args,
                            geometry_hash,
                            IndexingFom(
                                frames=0,
                                hits=0,
                                indexed_frames=0,
                                indexed_crystals=0,
                                detector_shift_x_mm=None,
                                detector_shift_y_mm=None,
                            ),
                        )
                    images_total = 0
                    with Path(f"job-{indexamajig_job_id}.lst").open(
                        "w", encoding="utf-8"
                    ) as input_file:
                        for event_line in event_batch:
                            input_file.write(f"{event_line}\n")
                            images_total += 1
                    with db:
                        db.execute(
                            "INSERT INTO IndexamajigJob (job_array_id, job_id, start_idx, state, images_total, images_processed) VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                job_array_id,
                                indexamajig_job_id,
                                indexamajig_job_id * IMAGES_PER_JOB,
                                "queued",
                                images_total,
                                0,
                            ),
                        )
                    indexamajig_job_id += 1
                    if indexamajig_job_id % INDEXAMAJIG_JOBS_PER_JOB_ARRAY == 0:
                        job_array_id += 1
                        job_array_ids.add(job_array_id)
                logger.info(f"all {indexamajig_job_id} indexamajig jobs added")
                logger.info("<= list_events finished!")
    return job_array_ids


def start_job_array_local(
    job_array_id: int,
    environment: dict[str, str],
) -> JobArrayLocal:
    cwd = Path(os.getcwd())
    stdout_obj = (cwd / f"job-{job_array_id}-stdout.txt").open("w")
    stderr_obj = (cwd / f"job-{job_array_id}-stderr.txt").open("w")
    # pylint: disable=consider-using-with
    proc = subprocess.Popen(
        [__file__], env=environment, stdout=stdout_obj, stderr=stderr_obj
    )
    return JobArrayLocal(proc)


def start_job_array_slurm(
    args: PrimaryArgs,
    db: sqlite3.Connection,
    job_array_id: int,
    script_file_contents: str,
    environment: dict[str, str],
) -> JobArraySlurm:
    number_of_indexamajig_jobs = db_execute_timed(
        db,
        "SELECT COUNT(*) FROM IndexamajigJob WHERE job_array_id = ?",
        (job_array_id,),
    ).fetchone()[0]
    cwd = Path(os.getcwd())
    request_json = {
        "job": {
            "partition": args.slurm_partition_to_use,
            "name": f"ix_{args.amarcord_indexing_result_id}_{job_array_id}",
            "nodes": 1,
            "array": f"0-{number_of_indexamajig_jobs-1}",
            "current_working_directory": str(cwd),
            "environment": environment,
            # This is in minutes
            "time_limit": 360,
            "standard_output": (str(cwd / f"job-{job_array_id}-%a-stdout.txt")),
            "standard_error": (str(cwd / f"job-{job_array_id}-%a-stderr.txt")),
        },
        "script": script_file_contents,
    }
    req = request.Request(
        f"{MAXWELL_URL}/job/submit",
        method="POST",
        headers=args.maxwell_headers,
        data=json.dumps(request_json).encode("utf-8"),
    )

    logger.info(
        "starting job array, stderr will land in "
        + str(str(cwd / f"job-{job_array_id}-%a-stderr.txt"))
    )
    try:
        with request.urlopen(req) as response:
            response_content_raw = response.read().decode("utf-8")
            logger.info(f"submitted job for job array id {job_array_id}")
            response_content_json = json.loads(response_content_raw)
            assert not response_content_json["errors"]
            job_id = response_content_json["job_id"]
            assert isinstance(job_id, int)
            return JobArraySlurm(job_id)
    except urllib.error.HTTPError as e:
        logger.exception(
            f"HTTP error sending job for {job_array_id}: {e.fp.read().decode('utf-8')}"
        )
        raise
    except:
        logger.exception(f"unknown error sending job for {job_array_id}")
        raise


def start_job_array(
    args: PrimaryArgs,
    db: sqlite3.Connection,
    cell_file: None | Path,
    script_file_contents: str,
    job_array_id: int,
) -> JobArray:
    environment: dict[str, str] = {
        "PATH": "/bin:/usr/bin/:/usr/local/bin/",
        "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
        OFF_INDEX_ENVIRON_JOB_STYLE: OFF_INDEX_ENVIRON_JOB_STYLE_SECONDARY,
        OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID: str(
            args.amarcord_indexing_result_id
        ),
        OFF_INDEX_ENVIRON_SECONDARY_JOB_ARRAY_ID: str(job_array_id),
        OFF_INDEX_ENVIRON_GEOMETRY_FILE: str(args.geometry_file),
        OFF_INDEX_ENVIRON_CRYSTFEL_PATH: str(args.crystfel_path),
        OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS: args.indexamajig_params,
        OFF_INDEX_ENVIRON_USE_SLURM: "True" if args.use_slurm else "False",
    }
    if cell_file is not None:
        environment[OFF_INDEX_ENVIRON_SECONDARY_CELL_FILE] = str(cell_file)

    if args.use_slurm:
        return start_job_array_slurm(
            args,
            db,
            job_array_id,
            script_file_contents,
            environment,
        )
    return start_job_array_local(job_array_id, environment)


def get_all_local_job_stati(job_array_pid: JobArrayLocal) -> list[str]:
    return_code = job_array_pid.wait()
    return ["RUNNING"] if return_code is None else []


def get_all_job_stati(
    args: PrimaryArgs, job_array_workload_manager_id: JobArray
) -> list[str]:
    if isinstance(job_array_workload_manager_id, JobArraySlurm):
        return get_all_slurm_job_stati(args, job_array_workload_manager_id)
    return get_all_local_job_stati(job_array_workload_manager_id)


def get_all_slurm_job_stati(
    args: PrimaryArgs, job_array_id: JobArraySlurm
) -> list[str]:
    req = request.Request(
        f"{MAXWELL_URL}/jobs", method="GET", headers=args.maxwell_headers
    )
    with request.urlopen(req) as response:
        response_content_raw = response.read().decode("utf-8")
        response_content_json = json.loads(response_content_raw)
        result = []
        for job in response_content_json["jobs"]:
            if (
                job["array_job_id"] > 0
                and job["array_job_id"] == job_array_id.job_id
                or job["job_id"] == job_array_id.job_id
            ):
                result.append(job["job_state"])
        return result


@dataclass(frozen=True)
class JobArrayFailure:
    jobs_with_errors: dict[int, str]


def run_job_array(
    args: PrimaryArgs,
    db: sqlite3.Connection,
    cell_file: None | Path,
    geometry_hash: str,
    script_file_contents: str,
    job_array_id: int,
) -> JobArrayFailure | IndexingFom:
    job_array_workload_manager_id = start_job_array(
        args, db, cell_file, script_file_contents, job_array_id
    )

    all_indexamajig_job_ids = set(
        r[0]
        for r in db_execute_timed(
            db,
            "SELECT job_id FROM IndexamajigJob WHERE job_array_id = ?",
            (job_array_id,),
        )
    )

    cooldown_iterations: None | int = None
    previous_images_processed: None | int = None
    POLL_SLEEP_S = 5.0

    def retrieve_foms_for_this_job() -> IndexingFom:
        (
            images_processed_so_far,
            hits,
            indexed_frames,
            indexed_crystals,
        ) = db_execute_timed(
            db,
            "SELECT SUM(images_processed), SUM(hits), SUM(indexed_frames), SUM(indexed_crystals) FROM IndexamajigJob WHERE job_array_id = ?",
            (job_array_id,),
        ).fetchone()
        return IndexingFom(
            frames=images_processed_so_far,
            hits=hits,
            indexed_frames=indexed_frames,
            indexed_crystals=indexed_crystals,
            detector_shift_x_mm=None,
            detector_shift_y_mm=None,
        )

    while True:
        job_stati = get_all_job_stati(args, job_array_workload_manager_id)

        failed_jobs = db_execute_timed(
            db,
            "SELECT job_id, error_log FROM IndexamajigJob WHERE state=? AND job_array_id = ?",
            ("failed", job_array_id),
        ).fetchall()

        if failed_jobs:
            logger.info("some jobs failed, cancelling")
            cancel_job(args, job_array_workload_manager_id)
            return JobArrayFailure(jobs_with_errors={x[0]: x[1] for x in failed_jobs})

        workload_manager_running = [
            x for x in job_stati if x in _SLURM_JOB_STATUS_RUNNING
        ]
        # If we either have no jobs on Slurm or they are all finished
        if not job_stati or all(
            job_status not in _SLURM_JOB_STATUS_RUNNING for job_status in job_stati
        ):
            logger.info("no running jobs anymore, checking if all successful")

            successful_indexamajig_job_ids: set[int] = set(
                x[0]
                for x in db_execute_timed(
                    db,
                    "SELECT job_id FROM IndexamajigJob WHERE state=? AND job_array_id = ?",
                    ("success", job_array_id),
                )
            )
            if successful_indexamajig_job_ids == all_indexamajig_job_ids:
                logger.info("all indexing jobs successful!")
                return retrieve_foms_for_this_job()
            logger.info(
                "some indexing jobs were not successful: "
                + ", ".join(
                    str(s)
                    for s in (all_indexamajig_job_ids - successful_indexamajig_job_ids)
                )
                + ", listing the stati"
            )
            have_running_jobs = False
            for job_id, state in db_execute_timed(
                db,
                "SELECT job_id, state FROM IndexamajigJob WHERE state!=? AND job_array_id = ?",
                ("success", job_array_id),
            ):
                logger.info(f"job {job_id} has status {state}")
                if state == "running":
                    have_running_jobs = True
            if have_running_jobs and (
                cooldown_iterations is None or cooldown_iterations < 2
            ):
                logger.info(
                    f"giving running jobs some time to finish (iterations {cooldown_iterations})"
                )
                cooldown_iterations = (
                    0 if cooldown_iterations is None else cooldown_iterations + 1
                )
            else:
                return JobArrayFailure(jobs_with_errors={})

        (
            images_processed_so_far,
            images_total,
            hits,
            indexed_frames,
            indexed_crystals,
        ) = db_execute_timed(
            db,
            "SELECT SUM(images_processed), SUM(images_total), SUM(hits), SUM(indexed_frames), SUM(indexed_crystals) FROM IndexamajigJob WHERE job_array_id = ?",
            (job_array_id,),
        ).fetchone()

        if previous_images_processed is not None:
            images_per_second = max(
                1, (images_processed_so_far - previous_images_processed) / POLL_SLEEP_S
            )
            images_to_go = images_total - images_processed_so_far
            seconds_to_go = round(images_to_go / images_per_second)
        else:
            images_per_second = 1
            seconds_to_go = 0.0
        previous_images_processed = images_processed_so_far

        end_time = datetime.fromtimestamp(time() + seconds_to_go)

        running_jobs = db_execute_timed(
            db,
            "SELECT COUNT(*) FROM IndexamajigJob WHERE state = ? AND job_array_id = ?",
            ("running", job_array_id),
        ).fetchone()[0]
        if hits is not None:
            logger.info(
                f"{images_processed_so_far}/{images_total}, "
                + f"{images_per_second}fps, {hits} hits (hr {hits / max(1, images_processed_so_far) * 100:.2f}%), "
                + f"{indexed_frames} indexed (ir {indexed_frames / max(1, hits) * 100:.2f}%), {indexed_crystals} crystals "
                + f" - ends {end_time.strftime('%d-%b-%Y %H:%M:%S')} ({seconds_to_go}s to go), "
                + f"{running_jobs}/{len(workload_manager_running)} running jobs in DB/SLURM"
            )
            write_status_still_running(
                args,
                geometry_hash,
                IndexingFom(
                    frames=images_processed_so_far,
                    hits=hits,
                    indexed_frames=indexed_frames,
                    indexed_crystals=indexed_crystals,
                    detector_shift_x_mm=None,
                    detector_shift_y_mm=None,
                ),
            )
        else:
            logger.info(
                f"no images processed so far, {running_jobs} running jobs in DB"
            )
            write_status_still_running(
                args,
                geometry_hash,
                IndexingFom(
                    frames=0,
                    hits=0,
                    indexed_frames=0,
                    indexed_crystals=0,
                    detector_shift_x_mm=None,
                    detector_shift_y_mm=None,
                ),
            )

        sleep(POLL_SLEEP_S)


def parse_secondary_args() -> SecondaryArgs:
    return SecondaryArgs(
        use_slurm=os.environ[OFF_INDEX_ENVIRON_USE_SLURM] == "True",
        amarcord_api_url=os.environ.get(OFF_INDEX_ENVIRON_AMARCORD_API_URL),
        amarcord_indexing_result_id=int(
            os.environ[OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID]
        ),
        job_array_id=int(os.environ[OFF_INDEX_ENVIRON_SECONDARY_JOB_ARRAY_ID]),
        crystfel_path=Path(os.environ[OFF_INDEX_ENVIRON_CRYSTFEL_PATH]),
        geometry_file=Path(os.environ[OFF_INDEX_ENVIRON_GEOMETRY_FILE]),
        indexamajig_params=shlex.split(
            os.environ[OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS]
        ),
        use_auto_geom_refinement=(
            "--mille" in os.environ[OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS]
        ),
        cell_file=(
            Path(os.environ[OFF_INDEX_ENVIRON_SECONDARY_CELL_FILE])
            if OFF_INDEX_ENVIRON_SECONDARY_CELL_FILE in os.environ
            else None
        ),
    )


def parse_millepede_output(stdout: str) -> str | tuple[float, float]:
    logger.info("parsing millepede output")
    success = False
    x_translation_mm: None | float = None
    y_translation_mm: None | float = None
    X_TRANSLATION_REGEX_INPUT = r"x-translation ([+-]?[0-9.]+) mm"
    Y_TRANSLATION_REGEX_INPUT = r"y-translation ([+-]?[0-9.]+) mm"
    MILLEPEDE_SUCCEDED_INPUT = "Millepede succeeded"
    x_translation_regex = re.compile(X_TRANSLATION_REGEX_INPUT)
    y_translation_regex = re.compile(Y_TRANSLATION_REGEX_INPUT)

    for l in stdout.split("\n"):
        if not success:
            if MILLEPEDE_SUCCEDED_INPUT in l:
                success = True
            continue

        x_match = x_translation_regex.search(l)
        if x_match is not None:
            x_translation_mm = float(x_match.group(1))
        y_match = y_translation_regex.search(l)
        if y_match is not None:
            y_translation_mm = float(y_match.group(1))

    if not success:
        error_message = "no line matching " + MILLEPEDE_SUCCEDED_INPUT
        logger.warning(f"parsing millepede output failed: {error_message}:\n{stdout}")
        return error_message

    if x_translation_mm is None:
        error_message = "no line matching " + X_TRANSLATION_REGEX_INPUT
        logger.warning(f"parsing millepede output failed: {error_message}:\n{stdout}")
        return error_message
    if y_translation_mm is None:
        error_message = "no line matching " + Y_TRANSLATION_REGEX_INPUT
        logger.warning(f"parsing millepede output failed: {error_message}:\n{stdout}")
        return error_message
    return x_translation_mm, y_translation_mm


def determine_beamtime_json(args: OnlineArgs, p: Path) -> tuple[Path, dict[str, Any]]:
    def find_metadata_json(p: Path) -> None | Path:
        all_files = list(p.glob("beamtime-metadata-*.json"))
        return None if not all_files else all_files[0]

    json_file = find_searching_upwards(p, find_metadata_json)

    if json_file is None:
        exit_with_error(
            args,
            f"Couldn't find beamtime-metadata*.json relative to {p}",
        )
    try:
        with json_file.open("r", encoding="utf-8") as f:
            return json_file, json.load(f)  # type: ignore[no-any-return]
    except Exception as e:
        exit_with_error(
            args,
            f"Error reading beamtime metadata json {json_file}: {e}",
        )


def run_align_detector(
    args: PrimaryArgs | OnlineArgs,
    millepede_files_dir: Path,
    geometry_file_destination: Path,
) -> None | tuple[float, float]:
    with TemporaryDirectory() as tempdir:
        with set_directory(tempdir):
            align_detector_binary = f"{args.crystfel_path}/bin/align_detector"
            if not Path(align_detector_binary).is_file():
                logger.error(
                    f"error running align_detector: binary {align_detector_binary} doesn't exist"
                )
                return None
            mille_bin_files: list[Path] = []

            if not millepede_files_dir.is_dir():
                logger.info(
                    f"directory {millepede_files_dir} does not exist - so we don't have any millepede output? listing files in cwd:"
                )
                for f in Path(".").iterdir():
                    logger.info(f"file {f}")
                logger.info("listing done")
                return None

            logger.info(f"iterating over directories in files in {millepede_files_dir}")

            MILLE_BIN_GLOB = "mille-data-*.bin"
            for mille_bin_dir in millepede_files_dir.iterdir():
                mille_bin_files.extend(mille_bin_dir.glob(MILLE_BIN_GLOB))

            if not mille_bin_files:
                logger.info(f"found no files matching {MILLE_BIN_GLOB}")
                return None

            # Outputting all args, including the .bin files, is way too much log spam.
            align_detector_args_prefix: list[str] = [
                align_detector_binary,
                "-i",
                str(args.geometry_file),
                "-o",
                str(geometry_file_destination),
                "--level=0",
            ]
            align_detector_args: list[str] = align_detector_args_prefix + [
                str(f) for f in mille_bin_files
            ]
            logger.info(
                f"found files matching {MILLE_BIN_GLOB}, running "
                + " ".join(align_detector_args_prefix)
                + " $bin_files"
            )
            try:
                align_detector_environ = os.environ.copy()
                align_detector_environ["PATH"] = (
                    f"{args.crystfel_path}/bin:{align_detector_environ['PATH']}"
                )
                completed_process = subprocess.run(
                    align_detector_args,
                    check=False,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    encoding="utf-8",
                    env=align_detector_environ,
                )
                logger.info(
                    f"completed align_detector call, return code {completed_process.returncode}"
                )
                detector_shift = parse_millepede_output(completed_process.stdout)
                if isinstance(detector_shift, str):
                    logger.error(
                        f"error running align_detector: output didn't parse correctly: {detector_shift}"
                    )
                    return None

                logger.info(
                    f"parsing millepede output succeeded, detector shift: {detector_shift}"
                )

                return detector_shift
            except Exception:
                logger.exception("error running align_detector")
                return None


@dataclass(frozen=True)
class BeamtimeMetadata:
    asapo_token: str
    asapo_endpoint: str
    beamtime_id: str


def read_beamtime_metadata(args: OnlineArgs) -> BeamtimeMetadata:
    beamtime_json_path, beamtime_json = determine_beamtime_json(
        args, Path(".").resolve()
    )
    beamtime_id = beamtime_json.get(METADATA_KEY_BEAMTIME_ID)
    if beamtime_id is None:
        exit_with_error(
            args,
            f'Couldn\'t find "{METADATA_KEY_BEAMTIME_ID}" in beamtime metadata json',
        )
    core_path = beamtime_json.get(METADATA_KEY_CORE_PATH)
    if core_path is None:
        exit_with_error(
            args,
            f'Couldn\'t find "{METADATA_KEY_CORE_PATH}" in beamtime metadata json',
        )
    asapo = beamtime_json.get(METADATA_KEY_ASAPO)
    if asapo is None:
        exit_with_error(
            args, f'Couldn\'t find "{METADATA_KEY_ASAPO}" in beamtime metadata json'
        )
    asapo_token_raw = asapo.get(METADATA_KEY_ASAPO_TOKEN_PATH)
    if asapo_token_raw is None:
        exit_with_error(
            args,
            f'Couldn\'t find "{METADATA_KEY_ASAPO}/{METADATA_KEY_ASAPO_TOKEN_PATH}" in beamtime metadata json',
        )
    asapo_endpoint = asapo.get(METADATA_KEY_ASAPO_ENDPOINT)
    if asapo_endpoint is None:
        exit_with_error(
            args,
            f'Couldn\'t find "{METADATA_KEY_ASAPO}/{METADATA_KEY_ASAPO_ENDPOINT}" in beamtime metadata json',
        )
    try:
        with (beamtime_json_path.parent / asapo_token_raw).open(
            "r", encoding="utf-8"
        ) as f:
            asapo_token_content = f.read().strip()
    except Exception as e:
        exit_with_error(
            args, f"tried to read asapo token from {asapo_token_raw}, but failed: {e}"
        )
    return BeamtimeMetadata(
        asapo_token=asapo_token_content,
        asapo_endpoint=asapo_endpoint,
        beamtime_id=beamtime_id,
    )


def run_online(args: OnlineArgs) -> None:
    logger.info(f"running in online mode, arguments: {args}")

    if args.geometry_file is None:
        resolved_geometry = find_geometry(Path(".").resolve())
    else:
        resolved_geometry = args.geometry_file

    if resolved_geometry is None:
        exit_with_error(
            args,
            f"did not find any geometry file relative to current dir {Path('.').resolve()}, exiting.",
        )

    args = replace(args, geometry_file=resolved_geometry)
    logger.info(f"using the following geometry: {resolved_geometry}")

    try:
        geometry_hash = crystfel_geometry_hash(resolved_geometry)
    except:
        exit_with_error(args, "cannot resolve geometry hash")

    if not args.crystfel_path.is_dir():
        exit_with_error(
            args, f"crystfel path {args.crystfel_path} doesn't exist, exiting."
        )

    try:
        crystfel_version = determine_crystfel_version(args.crystfel_path)
    except:
        exit_with_error(args, "cannot resolve CrystFEL version")

    logger.info(f"determined CrystFEL version {crystfel_version}")

    beamtime_metadata = read_beamtime_metadata(args)

    work_dir = Path(f"indexing-{args.amarcord_indexing_result_id}-work")
    work_dir.mkdir()
    logger.info(f"created work dir {work_dir}, switching")

    os.chdir(work_dir)

    cell_file: None | Path
    if args.cell_description is not None and args.cell_description.strip():
        parsed_cell_description = parse_cell_description(args.cell_description)
        if parsed_cell_description is None:
            exit_with_error(
                args, f"cell description {args.cell_description} is invalid"
            )
        cell_file = Path(f"{args.amarcord_indexing_result_id}.cell")
        write_cell_file(parsed_cell_description, cell_file)
    else:
        cell_file = None

    cpu_count = os.cpu_count()
    cpu_count_weighted = (
        int(cpu_count * args.cpu_count_multiplier)
        if cpu_count is not None and args.cpu_count_multiplier is not None
        else 96
    )
    cmd_line: list[str] = [
        f"{args.crystfel_path}/bin/indexamajig",
        f"--geometry={args.geometry_file}",
        "-j",
        str(cpu_count_weighted),
        "-o",
        str(args.stream_file),
    ] + args.indexamajig_params
    if _DESY_TEMP_DIR.is_dir():
        cmd_line.append(f"--temp-dir={_DESY_TEMP_DIR}")
    if cell_file is not None:
        cmd_line.append(f"--pdb={cell_file}")
    if args.use_auto_geom_refinement:
        mille_dir = Path(f"{args.amarcord_indexing_result_id}-millepede-files")
        logging.info(f"creating mille dir {mille_dir}")
        mille_dir.mkdir(exist_ok=True, parents=True)
        cmd_line.extend([f"--mille-dir={mille_dir}"])
    cmd_line.extend(
        [
            "--data-format=seedee",
            f"--asapo-endpoint={beamtime_metadata.asapo_endpoint}",
            f"--asapo-token={beamtime_metadata.asapo_token}",
            f"--asapo-beamtime={beamtime_metadata.beamtime_id}",
            f"--asapo-source={args.asapo_source}",
            # for only hits stream
            # "--asapo-output-stream",
            "--asapo-consumer-timeout=3000",
            "--no-data-timeout=15",
            f"--asapo-stream={args.external_run_id}",
            "--asapo-wait-for-stream",
            "--asapo-group=online",
        ]
    )
    logger.info(f"indexamajig command line: {shlex.join(cmd_line)}")
    early_exit = False
    indexamajig_environ = os.environ.copy()
    indexamajig_environ["PATH"] = (
        f"{args.crystfel_path}/bin:{indexamajig_environ['PATH']}"
    )
    with subprocess.Popen(
        cmd_line,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        bufsize=1,
        env=indexamajig_environ,
    ) as proc:
        logger.info("process started, reading lines")
        write_status_still_running(
            args,
            geometry_hash,
            IndexingFom(
                frames=0,
                hits=0,
                indexed_frames=0,
                indexed_crystals=0,
                detector_shift_x_mm=None,
                detector_shift_y_mm=None,
            ),
        )
        images = 0
        hits = 0
        indexable = 0
        crystals = 0
        previous_time: None | float = None
        start_time = time()
        while True:
            assert proc.stdout is not None
            line = proc.stdout.readline()
            if not line:
                break
            this_time = time()
            logger.info(line)
            match = _INDEXING_RE.search(line)
            if match is None:
                continue
            try:
                new_images = int(match.group(1))
                if previous_time is None:
                    previous_time = this_time
                else:
                    image_diff = new_images - images
                    fps = image_diff / (this_time - previous_time)

                    # if the FPS is too low and we have the "fps
                    # killer" on, and the run has been going on for a
                    # while then we might as well kill it
                    if (
                        fps < 2
                        and _FPS_KILLER_ONLINE_SECONDS != 0  # type: ignore
                        and this_time - start_time > _FPS_KILLER_ONLINE_SECONDS
                    ):
                        logger.warning(
                            f"fps too low and fps killer is on: {fps}, killing process"
                        )
                        proc.send_signal(signal.SIGUSR1)
                        early_exit = True
                        break
                images = new_images
                hits = int(match.group(2))
                indexable = int(match.group(3))
                crystals = int(match.group(4))
            except:
                logger.warning(f"indexing log line, but invalid format: {line}")
                continue
            logger.info(f"{images} image(s)")

            write_status_still_running(
                args,
                geometry_hash,
                IndexingFom(
                    frames=images,
                    hits=hits,
                    indexed_frames=indexable,
                    indexed_crystals=crystals,
                    detector_shift_x_mm=None,
                    detector_shift_y_mm=None,
                ),
            )

        proc.wait()
        if not early_exit and proc.returncode != 0:
            exit_with_error(
                args, f"indexamajig process return code is {proc.returncode}"
            )

        final_fom = IndexingFom(
            frames=images,
            hits=hits,
            indexed_frames=indexable,
            indexed_crystals=crystals,
            detector_shift_x_mm=None,
            detector_shift_y_mm=None,
        )
    logger.info("process completed")

    if args.use_auto_geom_refinement:
        logger.info("running align_detector")
        geometry_file_destination = str(args.stream_file.with_suffix(".geom").resolve())
        detector_shifts = run_align_detector(
            args,
            millepede_files_dir=Path(
                f"{args.amarcord_indexing_result_id}-millepede-files"
            ).resolve(),
            geometry_file_destination=Path(geometry_file_destination),
        )
        if detector_shifts is not None:
            logger.info(f"detector shift: {detector_shifts[0]}, {detector_shifts[1]}")
        else:
            detector_shifts = [None, None]
    else:
        detector_shifts = [None, None]
        geometry_file_destination = ""

    if final_fom.indexed_frames > 100:
        logger.info("generating histograms")
        try:
            graphs_output = generate_graphs(
                args.gnuplot_path, args.amarcord_indexing_result_id, args.stream_file
            )
        except:
            logger.exception("could not generate graphs")
            graphs_output = None

        final_fom = replace(
            final_fom,
            detector_shift_x_mm=detector_shifts[0],
            detector_shift_y_mm=detector_shifts[1],
        )
    else:
        logger.info(
            f"not generating histograms, not enough indexed frames: {final_fom.indexed_frames}"
        )
        graphs_output = None

    if graphs_output is not None:
        unit_cell_histograms_id = upload_file(args, graphs_output.unit_cell_histograms)

        final_fom = replace(
            final_fom,
            unit_cell_histograms_id=unit_cell_histograms_id,
        )

    write_status_success(
        args=args,
        program_version=crystfel_version,
        geometry_hash=geometry_hash,
        generated_geometry_file=geometry_file_destination,
        line=final_fom,
    )


def run_secondary(args: SecondaryArgs) -> None:
    logger.info(f"running in secondary mode, arguments: {args}")

    db = sqlite3.connect(f"{args.amarcord_indexing_result_id}.db", timeout=120.0)
    job_index = int(os.environ["SLURM_ARRAY_TASK_ID"]) if args.use_slurm else 0
    logger.info(f"determining job id for indexing job index {job_index}")
    job_id, start_idx = db_execute_timed(
        db,
        "SELECT job_id, start_idx FROM IndexamajigJob WHERE job_array_id = ? ORDER BY start_idx ASC",
        (args.job_array_id,),
    ).fetchall()[job_index]
    logger.info(f"job id is {job_id}, start idx is {start_idx}")

    lst_file_name = f"job-{job_id}.lst"
    cpu_count = os.cpu_count()
    cpu_count_weighted = max(1, int(cpu_count if cpu_count is not None else 1 * 0.8))
    cmd_line: list[str] = [
        f"{args.crystfel_path}/bin/indexamajig",
        f"--input={lst_file_name}",
        f"--serial-start={start_idx}",
        f"--output=job-{job_id}.stream",
        f"--geometry={args.geometry_file}",
        "-j",
        str(cpu_count_weighted),
    ] + args.indexamajig_params
    if _DESY_TEMP_DIR.is_dir():
        cmd_line.append(f"--temp-dir={_DESY_TEMP_DIR}")
    if args.cell_file is not None:
        cmd_line.append(f"--pdb={args.cell_file}")
    if args.use_auto_geom_refinement:
        mille_dir = Path(f"{args.amarcord_indexing_result_id}-millepede-files") / str(
            job_id
        )
        logging.info(f"creating mille dir {mille_dir}")
        mille_dir.mkdir(exist_ok=True, parents=True)
        cmd_line.extend([f"--mille-dir={mille_dir}"])
    logger.info(f"indexamajig command line: {shlex.join(cmd_line)}")
    try:
        db_execute_timed(
            db,
            "UPDATE IndexamajigJob SET state=? WHERE job_id=?",
            ("running", job_id),
        )
        db.commit()
    except sqlite3.OperationalError:
        logger.exception("initial update failed because of locked db")
        raise
    early_exit = False
    indexamajig_environ = os.environ.copy()
    indexamajig_environ["PATH"] = (
        f"{args.crystfel_path}/bin:{indexamajig_environ['PATH']}"
    )
    with subprocess.Popen(
        cmd_line,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        bufsize=1,
        env=indexamajig_environ,
    ) as proc:
        logger.info("process started, reading lines")
        images = 0
        hits = 0
        indexable = 0
        crystals = 0
        previous_time: None | float = None
        while True:
            assert proc.stdout is not None
            line = proc.stdout.readline()
            if not line:
                break
            this_time = time()
            logger.info(line)
            match = _INDEXING_RE.search(line)
            if match is None:
                continue
            try:
                new_images = int(match.group(1))
                if previous_time is None:
                    previous_time = this_time
                else:
                    image_diff = new_images - images
                    fps = image_diff / (this_time - previous_time)

                    if fps < 2 and _FPS_KILLER_OFFLINE:
                        logger.warning(
                            f"fps too low and fps killer is on: {fps}, killing process"
                        )
                        proc.send_signal(signal.SIGUSR1)
                        early_exit = True
                        break
                images = new_images
                hits = int(match.group(2))
                indexable = int(match.group(3))
                crystals = int(match.group(4))
            except:
                logger.warning(f"indexing log line, but invalid format: {line}")
                continue
            logger.info(f"{images} image(s)")
            db_execute_timed(
                db,
                "UPDATE IndexamajigJob SET images_processed=?, hits=?, indexed_frames=?, indexed_crystals=? WHERE job_id=?",
                (images, hits, indexable, crystals, job_id),
            )
            db.commit()

        proc.wait()
        if not early_exit and proc.returncode != 0:
            logger.info(f"process return code is {proc.returncode}")
            db_execute_timed(
                db,
                "UPDATE IndexamajigJob SET error_log=?, state=? WHERE job_id=?",
                ("\n".join(log_list), "failed", job_id),  # type: ignore
            )
            db.commit()
            return
    logger.info("process completed")

    db_execute_timed(
        db,
        "UPDATE IndexamajigJob SET state=?, images_processed=?, hits=?, indexed_frames=?, indexed_crystals=? WHERE job_id=?",
        ("success", images, hits, indexable, crystals, job_id),  # type: ignore
    )
    db.commit()
    logger.info(f"committed transaction, job {job_id} is success now")


def find_searching_upwards(
    f: Path, finder: Callable[[Path], None | Path]
) -> None | Path:
    if not f.is_file() and not f.is_dir():
        logger.error(f"base path used to find a directory; path was: {f}")
        return None
    if f.is_file():
        f = f.parent

    guess = finder(f)
    if guess is not None:
        return guess
    while f != f.parent:
        f = f.parent
        guess = finder(f)
        if guess is not None:
            return guess
    return None


def find_geometry(f: Path) -> None | Path:
    def relative_geom_file(g: Path) -> None | Path:
        geometry_file = (g / "shared" / "geometry.geom").resolve()
        if geometry_file.is_file():
            return geometry_file
        return None

    return find_searching_upwards(f, relative_geom_file)


def write_gnuplot_script(target: IO[bytes]) -> None:
    target.write(
        """
# Set the output to a PNG file
set terminal pngcairo size 1800,1100 enhanced font 'Verdana,10'
set output myoutput

# Set the title of the plot
set encoding utf8

# Label the axes
# set xlabel "Length"
set ylabel "Frequency"

# Set the style to histogram and fill style
set style data histogram
set style histogram cluster gap 1
set style fill solid border

# Set the width of the bars
set boxwidth 0.9

# Define the bin width
bin_width = 0.01
bin(x, width) = width * floor(x / width) + bin_width / 2.0

# Set boxwidth
set boxwidth bin_width

set table "tempfile"
plot inputfile using (bin(column(1), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "A"

set table "tempfile"
plot inputfile using (bin(column(2), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "B"

set table "tempfile"
plot inputfile using (bin(column(3), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "C"

set table "tempfile"
plot inputfile using (bin(column(4), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "ALPHA"

set table "tempfile"
plot inputfile using (bin(column(5), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "BETA"

set table "tempfile"
plot inputfile using (bin(column(6), bin_width)):(1.0) smooth freq with boxes notitle
unset table
stats "tempfile" using 1:2 prefix "GAMMA"

set grid xtics
set ytics auto nomirror

set multiplot layout 2,3 rowsfirst

set title "A"
set xtics auto nomirror out rotate
set x2range [A_min_x-5:A_max_x+5]
set xrange [A_min_x-5:A_max_x+5]
set x2tics out rotate (A_pos_max_y A_pos_max_y)
set style fill solid border lt 1
plot inputfile using (bin(column(1), bin_width)):(1.0) smooth freq with boxes notitle

set title "B"
set xtics auto nomirror out rotate
set x2range [B_min_x-5:B_max_x+5]
set xrange [B_min_x-5:B_max_x+5]
set x2tics out rotate (B_pos_max_y B_pos_max_y)
set style fill solid border lt 2
plot inputfile using (bin(column(2), bin_width)):(1.0) smooth freq with boxes notitle

set title "C"
set xtics auto nomirror out rotate
set x2range [C_min_x-5:C_max_x+5]
set xrange [C_min_x-5:C_max_x+5]
set x2tics out rotate (C_pos_max_y C_pos_max_y)
set style fill solid border lt 3
plot inputfile using (bin(column(3), bin_width)):(1.0) smooth freq with boxes notitle

# set xlabel "Length"

set title "Alpha"
set style fill solid border lt 4
set xtics auto nomirror out rotate
set x2range [ALPHA_min_x-5:ALPHA_max_x+5]
set xrange [ALPHA_min_x-5:ALPHA_max_x+5]
set x2tics out rotate (ALPHA_pos_max_y ALPHA_pos_max_y)
plot inputfile using (bin(column(4), bin_width)):(1.0) smooth freq with boxes notitle
set title "Beta"
set xtics auto nomirror out rotate
set x2range [ALPHA_min_x-5:ALPHA_max_x+5]
set xrange [ALPHA_min_x-5:ALPHA_max_x+5]
set x2tics out rotate (ALPHA_pos_max_y ALPHA_pos_max_y)
set style fill solid border lt 5
plot inputfile using (bin(column(5), bin_width)):(1.0) smooth freq with boxes notitle
set title "Gamma"
set xtics auto nomirror out rotate
set x2range [ALPHA_min_x-5:ALPHA_max_x+5]
set xrange [ALPHA_min_x-5:ALPHA_max_x+5]
set x2tics out rotate (ALPHA_pos_max_y ALPHA_pos_max_y)
set style fill solid border lt 6
plot inputfile using (bin(column(6), bin_width)):(1.0) smooth freq with boxes notitle

unset multiplot
        """.encode(
            "utf-8"
        )
    )


@dataclass(frozen=True)
class GraphOutput:
    unit_cell_histograms: Path


def generate_graphs(
    gnuplot_path: None | Path, indexing_result_id: int, stream_file: Path
) -> GraphOutput:
    cell_description_file_path = Path(
        f"{indexing_result_id}-cell-description-for-gnuplot.txt"
    )

    with cell_description_file_path.open("wb+") as cell_description_file:
        with subprocess.Popen(
            ["grep", "Cell parameters", str(stream_file)],
            stdout=subprocess.PIPE,
        ) as grep_process:
            subprocess.run(
                ["awk", "{print($3, $4, $5, $7, $8, $9)}"],
                stdin=grep_process.stdout,
                stdout=cell_description_file,
                check=True,
            )

        # with tempfile.NamedTemporaryFile() as gnuplot_script:
        gnuplot_script_path = Path(f"{indexing_result_id}.gnuplot")
        logger.info(f"writing to file {gnuplot_script_path}")
        with gnuplot_script_path.open("wb+") as gnuplot_script:
            write_gnuplot_script(gnuplot_script)

        def run_gnuplot(output_file: str) -> None:
            cell_description_file.seek(0)

            gnuplot_args: list[str] = [
                "gnuplot" if gnuplot_path is None else str(gnuplot_path),
                "-e",
                f"myoutput='{output_file}'",
                "-e",
                f"inputfile='{cell_description_file_path}'",
                str(gnuplot_script_path),
            ]
            logger.info(f"gnuplot arguments: {gnuplot_args}")
            subprocess.check_output(gnuplot_args)

        run_gnuplot(f"{indexing_result_id}.png")

        return GraphOutput(unit_cell_histograms=Path(f"{indexing_result_id}.png"))


def determine_crystfel_version(crystfel_path: Path) -> str:
    output = subprocess.check_output(
        [f"{crystfel_path}/bin/indexamajig", "--version"], encoding="utf-8"
    )

    return output.split("\n", maxsplit=1)[0].replace("CrystFEL: ", "")


def upload_file(args: PrimaryArgs | OnlineArgs, file_path: Path) -> None | int:
    with file_path.open("rb") as file_obj:
        url = f"{args.amarcord_api_url}/api/files/simple/{file_path.suffix.replace('.', '')}"
        logger.info(f"uploading file to {url}")
        req = request.Request(url, method="POST", data=file_obj)

        try:
            with request.urlopen(req) as response:
                response_content = response.read().decode("utf-8")
                try:
                    parsed_response = json.loads(response_content)
                except:
                    exit_with_error(
                        args,
                        f"Couldn't parse output from file upload: {response_content}",
                    )
                file_id = parsed_response.get("id", None)
                if file_id is None or not isinstance(file_id, int):
                    exit_with_error(
                        args,
                        f"JSON response from file upload didn't contain the file ID: {response_content}",
                    )
                return file_id
        except:
            logger.exception(f"error uploading file {file_path}")
            return None


def run_primary(args: PrimaryArgs) -> None:
    logger.info(f"running in primary mode, arguments: {args}")
    logger.info(f"PATH is {os.environ.get('PATH')}")

    if not args.input_files:
        logger.error("input file list empty, exiting immediately")
        sys.exit(1)

    if args.geometry_file is None:
        resolved_geometry = find_geometry(args.input_files[0])
    else:
        resolved_geometry = args.geometry_file

    if resolved_geometry is None:
        logger.error("did not find any geometry file, exiting.")
        sys.exit(1)

    args = replace(args, geometry_file=resolved_geometry)
    logger.info(f"using the following geometry: {resolved_geometry}")

    try:
        geometry_hash = crystfel_geometry_hash(resolved_geometry)
    except:
        logger.exception("cannot resolve geometry hash")
        sys.exit(1)

    if not args.crystfel_path.is_dir():
        logger.error(f"crystfel path {args.crystfel_path} doesn't exist, exiting.")
        sys.exit(1)

    try:
        crystfel_version = determine_crystfel_version(args.crystfel_path)
    except:
        logger.exception("cannot resolve CrystFEL version")
        sys.exit(1)

    logger.info(f"determined CrystFEL version {crystfel_version}")

    work_dir = Path(f"indexing-{args.amarcord_indexing_result_id}-work")
    work_dir.mkdir()
    logger.info(f"created work dir {work_dir}, switching")

    os.chdir(work_dir)
    cell_file: None | Path
    if args.cell_description is not None and args.cell_description.strip():
        parsed_cell_description = parse_cell_description(args.cell_description)
        if parsed_cell_description is None:
            logger.error(f"cell description {args.cell_description} is invalid")
            sys.exit(1)
        cell_file = Path(f"{args.amarcord_indexing_result_id}.cell")
        write_cell_file(parsed_cell_description, cell_file)
    else:
        cell_file = None

    start_time = time()
    # timeout set to 60 because we have high contention
    db = sqlite3.connect(f"{args.amarcord_indexing_result_id}.db", timeout=60.0)

    with db:
        db.execute(
            """CREATE TABLE IndexamajigJob (
              job_array_id INTEGER,
              job_id INTEGER PRIMARY KEY,
              start_idx INTEGER,
              state TEXT,
              images_total INTEGER,
              images_processed INTEGER,
              hits INTEGER,
              indexed_frames INTEGER,
              indexed_crystals INTEGER,
              error_log TEXT
            )
            """
        )
        db.execute("CREATE INDEX job_state_index ON IndexamajigJob (state)")
        db.execute("CREATE INDEX job_array_id_index ON IndexamajigJob (job_array_id)")

    job_array_ids = initialize_db(args, geometry_hash, db)

    if not job_array_ids:
        logger.error("there are no jobs to run")
        exit_with_error(args, "no jobs to run")

    logger.info("database initialized!")

    with Path(__file__).open("r", encoding="utf-8") as script_file_obj:
        script_file_contents = script_file_obj.read()

    job_array_results: list[IndexingFom] = []
    for job_array_id in job_array_ids:
        logger.info(f"tackling job array {job_array_id}")

        job_array_result = run_job_array(
            args=args,
            db=db,
            cell_file=cell_file,
            geometry_hash=geometry_hash,
            script_file_contents=script_file_contents,
            job_array_id=job_array_id,
        )
        if isinstance(job_array_result, JobArrayFailure):
            logger.error(f"exiting early, job array {job_array_id} not successful")
            error_message = "the following jobs failed and had output:\n"
            for (
                job_id,
                subjob_error_message,
            ) in job_array_result.jobs_with_errors.items():
                error_message += f"job {job_id}:\n\n{subjob_error_message}\n\n"
            write_error_json(
                args,
                (
                    error_message
                    if job_array_result.jobs_with_errors
                    else "some sub-jobs failed, not sure how"
                ),
            )
            sys.exit(1)
        job_array_results.append(job_array_result)

        logger.info(f"job array {job_array_id} successful!")

    logger.info(
        "all job arrays finished - whole job finished, concatenating stream files"
    )

    with args.stream_file.open("wb") as concatenated_output:
        for job_array_id, job_id in db.execute(
            "SELECT job_array_id, job_id FROM IndexamajigJob"
        ):
            job_stream_file = f"job-{job_id}.stream"
            with open(job_stream_file, "rb") as single_stream:
                shutil.copyfileobj(single_stream, concatenated_output)
            os.unlink(job_stream_file)
            # This is a little spammy
            # logger.info(f"stream file for job {job_id} appended to {args.stream_file}")

    if args.use_auto_geom_refinement:
        logger.info("running align_detector")
        geometry_file_destination = str(args.stream_file.with_suffix(".geom").resolve())
        detector_shifts = run_align_detector(
            args,
            millepede_files_dir=Path(
                f"{args.amarcord_indexing_result_id}-millepede-files"
            ).resolve(),
            geometry_file_destination=Path(geometry_file_destination),
        )
        if detector_shifts is not None:
            logger.info(f"detector shift: {detector_shifts[0]}, {detector_shifts[1]}")
        else:
            detector_shifts = [None, None]
    else:
        detector_shifts = [None, None]
        geometry_file_destination = ""

    logger.info("generating histograms")
    try:
        graphs_output = generate_graphs(
            args.gnuplot_path, args.amarcord_indexing_result_id, args.stream_file
        )
    except:
        logger.exception("could not generate graphs")
        graphs_output = None

    final_fom = replace(
        reduce(add_indexing_fom, job_array_results),
        detector_shift_x_mm=detector_shifts[0],
        detector_shift_y_mm=detector_shifts[1],
    )

    if graphs_output is not None:
        unit_cell_histograms_id = upload_file(args, graphs_output.unit_cell_histograms)

        final_fom = replace(
            final_fom,
            unit_cell_histograms_id=unit_cell_histograms_id,
        )

    write_status_success(
        args=args,
        program_version=crystfel_version,
        geometry_hash=geometry_hash,
        generated_geometry_file=geometry_file_destination,
        line=final_fom,
    )

    clean_intermediate_files(args.amarcord_indexing_result_id)

    logger.info(f"done, finished in {time() - start_time}s")


def parse_primary_args() -> PrimaryArgs:
    glob_list: list[str] = json.loads(os.environ[OFF_INDEX_ENVIRON_INPUT_FILE_GLOBS])
    amarcord_indexing_result_id = int(
        os.environ[OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID]
    )
    use_slurm = os.environ[OFF_INDEX_ENVIRON_USE_SLURM] == "True"
    indexamajig_params = os.environ[OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS]
    gnuplot_path = os.environ[OFF_INDEX_ENVIRON_GNUPLOT_PATH]
    return PrimaryArgs(
        use_slurm=use_slurm,
        slurm_partition_to_use=os.environ[OFF_INDEX_ENVIRON_SLURM_PARTITION_TO_USE],
        workload_manager_job_id=(
            int(os.environ["SLURM_JOB_ID"]) if use_slurm else os.getpid()
        ),
        stream_file=Path(os.environ[OFF_INDEX_ENVIRON_STREAM_FILE]),
        amarcord_api_url=os.environ.get(OFF_INDEX_ENVIRON_AMARCORD_API_URL),
        input_files=list(Path(p) for glob_ in glob_list for p in glob.glob(glob_)),
        geometry_file=(
            Path(os.environ[OFF_INDEX_ENVIRON_GEOMETRY_FILE])
            if OFF_INDEX_ENVIRON_GEOMETRY_FILE in os.environ
            else None
        ),
        cell_description=os.environ.get(OFF_INDEX_ENVIRON_CELL_DESCRIPTION),
        use_auto_geom_refinement="--mille" in indexamajig_params,
        amarcord_indexing_result_id=amarcord_indexing_result_id,
        crystfel_path=Path(os.environ[OFF_INDEX_ENVIRON_CRYSTFEL_PATH]),
        maxwell_headers=(
            {
                "X-SLURM-USER-NAME": getpass.getuser(),
                "X-SLURM-USER-TOKEN": os.environ["SLURM_TOKEN"],
                "Content-Type": "application/json",
            }
            if use_slurm
            else {}
        ),
        indexamajig_params=indexamajig_params,
        gnuplot_path=Path(gnuplot_path) if gnuplot_path else None,
    )


def parse_online_args() -> OnlineArgs:
    amarcord_indexing_result_id = int(
        os.environ[OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID]
    )
    use_slurm = os.environ[OFF_INDEX_ENVIRON_USE_SLURM] == "True"
    gnuplot_path = os.environ[OFF_INDEX_ENVIRON_GNUPLOT_PATH]
    return OnlineArgs(
        workload_manager_job_id=(
            int(os.environ["SLURM_JOB_ID"]) if use_slurm else os.getpid()
        ),
        stream_file=Path(os.environ[OFF_INDEX_ENVIRON_STREAM_FILE]),
        amarcord_api_url=os.environ.get(OFF_INDEX_ENVIRON_AMARCORD_API_URL),
        asapo_source=os.environ[ON_INDEX_ENVIRON_ASAPO_SOURCE],
        cpu_count_multiplier=float(
            os.environ[ON_INDEX_ENVIRON_AMARCORD_CPU_COUNT_MULTIPLIER]
        ),
        geometry_file=(
            Path(os.environ[OFF_INDEX_ENVIRON_GEOMETRY_FILE])
            if OFF_INDEX_ENVIRON_GEOMETRY_FILE in os.environ
            else None
        ),
        cell_description=os.environ.get(OFF_INDEX_ENVIRON_CELL_DESCRIPTION),
        use_auto_geom_refinement="--mille" in OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS,
        amarcord_indexing_result_id=amarcord_indexing_result_id,
        crystfel_path=Path(os.environ[OFF_INDEX_ENVIRON_CRYSTFEL_PATH]),
        indexamajig_params=shlex.split(
            os.environ[OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS]
        ),
        external_run_id=int(os.environ[OFF_INDEX_ENVIRON_RUN_ID]),
        gnuplot_path=Path(gnuplot_path) if gnuplot_path else None,
    )


if __name__ == "__main__":
    if JOB_STYLE == OFF_INDEX_ENVIRON_JOB_STYLE_PRIMARY:
        run_primary(parse_primary_args())
    elif JOB_STYLE == OFF_INDEX_ENVIRON_JOB_STYLE_SECONDARY:
        run_secondary(parse_secondary_args())
    else:
        run_online(parse_online_args())
