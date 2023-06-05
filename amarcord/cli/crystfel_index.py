#!/software/python/3.10/bin/python3
import json
import logging
import os
import re
import subprocess
import sys
from base64 import b64decode
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import time
from typing import IO
from typing import Any
from typing import NoReturn
from urllib import request

METADATA_KEY_ASAPO_ENDPOINT = "endpoint"

METADATA_KEY_ASAPO = "asapo"

METADATA_KEY_ASAPO_TOKEN_PATH = "beamtimeTokenPath"

METADATA_KEY_BEAMTIME_ID = "beamtimeId"
METADATA_KEY_CORE_PATH = "corePath"
_INDEXING_RE = re.compile(
    r"(\d+) images processed, (\d+) hits \([^)]+\), (\d+) indexable \([^)]+\), (\d+) crystals, .*"
)


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


def make_cell_file_name(c: CrystFELCellFile) -> str:
    ua = c.unique_axis if c.unique_axis else "noaxis"
    return f"chemical_{c.lattice_type}_{c.centering}_{ua}_{c.a}_{c.b}_{c.c}_{c.alpha}_{c.beta}_{c.gamma}_{int(time())}.cell"


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


@dataclass(frozen=True)
class ParsedArgs:
    run_id: int
    job_id: int
    api_url: str
    stream_file: Path
    crystfel_path: Path
    cell_description: None | str
    dummy_h5_input: None | str


def read_path(j: dict[str, Any], key: str) -> Path:
    result_str = j.get(key)
    if result_str is None:
        exit_with_error(None, f"{key} missing in input")
    if not isinstance(result_str, str):
        exit_with_error(
            None,
            f"{key} not a string but {result_str} (type {type(result_str)})",
        )
    return Path(result_str)


def parse_predefined(s: bytes) -> ParsedArgs:
    j = json.loads(b64decode(s))
    assert isinstance(j, dict)
    stream_file_path = read_path(
        j, "stream-file"  # pyright: ignore[reportUnknownArgumentType]
    )
    if stream_file_path.is_file():
        exit_with_error(
            None,
            f"Stream file path {stream_file_path} already exists, cannot start indexing",
        )
    crystfel_path = read_path(
        j, "crystfel-path"  # pyright: ignore[reportUnknownArgumentType]
    )

    run_id = j.get("run-id")
    if run_id is None:
        exit_with_error(None, "run-id missing in input")
    if not isinstance(run_id, int):
        exit_with_error(
            None,
            f"run-id not an int but {run_id} (type {type(run_id)})",  # pyright: ignore[reportUnknownArgumentType]
        )

    job_id = j.get("job-id")
    if job_id is None:
        exit_with_error(None, "job-id missing in input")
    if not isinstance(job_id, int):
        exit_with_error(
            None,
            f"job-id not an int but {job_id} (type {type(job_id)})",  # pyright: ignore[reportUnknownArgumentType]
        )

    cell_description = j.get("cell-description")
    if cell_description is not None:
        if not isinstance(cell_description, str):
            exit_with_error(
                None,
                f"cell-description not a string but {cell_description} (type {type(cell_description)})",  # pyright: ignore[reportUnknownArgumentType]
            )

    return ParsedArgs(
        run_id=run_id,
        job_id=job_id,
        api_url=j.get("api-url"),  # type: ignore
        stream_file=stream_file_path,
        cell_description=cell_description,  # pyright: ignore[reportUnknownArgumentType]
        crystfel_path=crystfel_path,
        dummy_h5_input=j.get(  # pyright: ignore[reportUnknownArgumentType]
            "dummy-h5-input"
        ),
    )


predefined_args: None | bytes = None

# predefined_args_dict = {
#     "run-id": 1,
#     "job-id": 1,
#     "api-url": "http://localhost:5001",
#     "stream-file": "/tmp/index-test/output.stream",
#     "crystfel-path": "/tmp/index-test/crystfel",
#     "cell-description": "tetragonal P c (79.2 79.2 38.0) (90 90 90)",
# }
# predefined_args = b64encode(
#     json.dumps(predefined_args_dict, allow_nan=False).encode("utf-8")
# ).decode("utf-8")


def write_output_json(
    args: ParsedArgs, error: None | str, result: None | dict[str, Any]
) -> None:
    req = request.Request(
        f"{args.api_url}/api/indexing/{args.job_id}",
        data=json.dumps(
            {"error": error, "result": result}, allow_nan=False, indent=2
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


def write_status(args: ParsedArgs, line: IndexingFom, done: bool) -> None:
    write_output_json(
        args,
        error=None,
        result={
            "frames": line.frames,
            "hits": line.hits,
            "indexed_frames": line.indexed_frames,
            "indexed_crystals": line.indexed_crystals,
            "done": done,
        },
    )


def exit_with_error(args: None | ParsedArgs, message: str) -> NoReturn:
    logger.error(message)
    if args is not None:
        write_output_json(args, error=message, result=None)
    sys.exit(1)


def determine_beamtime_json(args: ParsedArgs, p: Path) -> dict[str, Any]:
    json_files = list(p.glob("beamtime-metadata-*.json"))
    if not json_files:
        if p.parent == p:
            exit_with_error(
                args,
                "Couldn't find beamtime-metadata*.json relative to current working directory",
            )
        return determine_beamtime_json(args, p.parent)
    try:
        with json_files[0].open("r", encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]
    except Exception as e:
        exit_with_error(
            args,
            f"Error reading beamtime metadata json {json_files[0]}: {e}",
        )


def generate_output(args: ParsedArgs) -> None:
    cell_file_directory = Path("./processed/cells")
    try:
        cell_file_directory.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        exit_with_error(
            args, f"error creating cell file directory {cell_file_directory}: " + str(e)
        )
    output_cell_file: None | Path = None
    if args.cell_description is not None:
        try:
            cell_description = parse_cell_description(args.cell_description)
        except Exception as e:
            exit_with_error(
                args, f'error parsing cell description "{args.cell_description}": {e}'
            )
        assert isinstance(cell_description, CrystFELCellFile)
        try:
            # noinspection PyUnboundLocalVariable
            output_cell_file = cell_file_directory / make_cell_file_name(
                cell_description
            )
            write_cell_file(cell_description, output_cell_file)
            logger.info(f"cell file written to {output_cell_file}")
        except Exception as e:
            exit_with_error(args, "error creating cell file " + str(e))

    beamtime_json = determine_beamtime_json(args, Path(".").absolute())
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
    run_indexamajig(
        args,
        core_path=Path(core_path),
        cell_file=output_cell_file,
        beamtime_id=beamtime_id,
        asapo_token=Path(core_path) / asapo_token_raw,
        asapo_endpoint=asapo_endpoint,
    )


def run_indexamajig(
    args: ParsedArgs,
    cell_file: None | Path,
    core_path: Path,
    asapo_token: Path,
    asapo_endpoint: str,
    beamtime_id: str,
) -> None:
    with asapo_token.open("r", encoding="utf-8") as f:
        asapo_token_content = f.read().strip()

    command_line_args = [
        f"{args.crystfel_path}/bin/indexamajig",
        "--peaks=peakfinder8",
        "--temp-dir=/scratch",
        "--min-snr=5",
        "--min-res=50",
        "--threshold=4",
        "--min-pix-count=2",
        "--max-pix-count=50",
        "--peakfinder8-fast",
        "--min-peaks=10",
        "--local-bg-radius=3",
        "--int-radius=4,5,7",
        "--indexing=asdf",
        "--cpu-pin",
        "--asdf-fast",
        f"-j{os.cpu_count()}",
        "--no-retry",
        "-g",
        f"{core_path}/shared/geometry.geom",
        "--profile",
        "-o",
        str(args.stream_file),
    ]
    if cell_file is not None:
        command_line_args.extend(["-p", str(cell_file)])
    # pylint: disable=consider-using-with
    input_tmpfile = NamedTemporaryFile() if args.dummy_h5_input is not None else None
    if args.dummy_h5_input:
        assert input_tmpfile is not None
        input_tmpfile.write(args.dummy_h5_input.encode("utf-8"))
        input_tmpfile.flush()
        command_line_args.append(f"--input={input_tmpfile.name}")
    else:
        command_line_args.extend(
            [
                "--data-format=seedee",
                f"--asapo-endpoint={asapo_endpoint}",
                f"--asapo-token={asapo_token_content}",
                f"--asapo-beamtime={beamtime_id}",
                "--asapo-source=haspp11e16m-100g",
                f"--asapo-stream={args.run_id}",
                "--asapo-wait-for-stream",
                "--asapo-group=online",
            ]
        )
    logging.info(f"starting indexamajig with command line: {command_line_args}")
    try:
        profile_directory = core_path / "processed" / "profiles"
        profile_directory.mkdir(parents=True, exist_ok=True)
        with (profile_directory / f"profile-{args.run_id}.log").open(
            "wb"
        ) as profile_file:
            with subprocess.Popen(
                command_line_args,
                stdout=profile_file,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                encoding="utf-8",
                bufsize=1,
            ) as proc:
                if args.dummy_h5_input is not None:
                    assert proc.stdin is not None
                    proc.stdin.write(args.dummy_h5_input)
                    proc.stdin.close()
                last_fom: None | IndexingFom = None
                assert proc.stderr is not None
                while True:
                    logger.info("reading line")
                    err_line = proc.stderr.readline()
                    logger.info(f"reading line finished: {err_line.rstrip()}")
                    if not err_line:
                        break
                    match = _INDEXING_RE.search(err_line)
                    if match is None:
                        continue
                    try:
                        images = int(match.group(1))
                        hits = int(match.group(2))
                        indexable = int(match.group(3))
                        crystals = int(match.group(4))
                    except:
                        logger.warning(
                            f"indexing log line, but invalid format: {err_line}"
                        )
                        continue
                    last_fom = IndexingFom(
                        frames=images,
                        hits=hits,
                        indexed_frames=indexable,
                        indexed_crystals=crystals,
                    )
                    logger.info(f"writing {last_fom}")
                    write_status(
                        args,
                        last_fom,
                        done=False,
                    )
                logger.info(f"process done writing final fom: {last_fom}")
                write_status(
                    args,
                    last_fom
                    if last_fom is not None
                    else IndexingFom(
                        frames=0, hits=0, indexed_frames=0, indexed_crystals=0
                    ),
                    done=True,
                )
                if input_tmpfile is not None:
                    input_tmpfile.close()
    except Exception as e:
        exit_with_error(args, f"error running indexamajig: {e}")


if __name__ == "__main__":
    if predefined_args is None:
        exit_with_error(None, "No predefined_args given")
    generate_output(parse_predefined(predefined_args))
