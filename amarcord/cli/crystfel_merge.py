#!/usr/bin/env python3
import json
import logging
import math
import multiprocessing
import os
import re
import shlex
import shutil
import subprocess
import sys
from base64 import b64decode
from dataclasses import dataclass
from pathlib import Path
from random import Random
from tempfile import NamedTemporaryFile
from typing import Any
from typing import BinaryIO
from typing import Final
from typing import Generator
from typing import Iterable
from typing import NoReturn
from typing import TypeVar
from urllib import request

_NUMBER_OF_COLUMNS_IN_COMPARE_SHELL_FILE: Final = 6
_NUMBER_OF_COLUMNS_IN_CHECK_SHELL_FILE: Final = 11
_HIGHRES_CUT_CCSTAR_THRESHOLD: Final = 0.5
_MAX_SHELLS_TO_TEST: Final = 30


def ccstar_compare_shell_file(i: int) -> Path:
    return Path(f"ccstar_shells_first_pass_{i}.dat")


DESIRED_NREFS_PER_SHELL: Final = 2000
RSPLIT_COMPARE_SHELL_FILE = Path("rsplit_shells.dat")
CCSTAR_COMPARE_SHELL_FILE = Path("ccstar_shells.dat")
CHECK_HKL_SHELL_FILE = Path("check.dat")
CC_COMPARE_SHELL_FILE = Path("cc_shells.dat")

EXCLUSION_MTZ = "input-mtz-after-rflag-exclusion.mtz"
POINTLESS_MTZ = "input-pointless.mtz"
UNIQUE_MTZ = "input-mtz-after-unique.mtz"
CAD_MTZ = "input-mtz-after-cad.mtz"
FREER_MTZ = "input-mtz-after-freerflag.mtz"
UNIQIFIED_MTZ = "input-mtz-uniqified.mtz"
RESCUT_MTZ = "input-mtz-rescut.mtz"
DIMPLE_OUT_MTZ = "output-dimple.mtz"
DIMPLE_OUT_PDB = "output-dimple.pdb"


def ccp4_run(ccp4_path: Path, args: list[str], input_: None | str = None) -> str:
    current_path = os.environ["PATH"]
    ccp4_env: dict[str, str] = {
        "CLIBD": f"{ccp4_path}/lib/data",
        "CLIBD_MON": f"{ccp4_path}/lib/data/monomers/",
        "CINCL": f"{ccp4_path}/include",
        "CCP4_SCR": "/tmp",
        "CCP4": str(ccp4_path),
        "PATH": f"{ccp4_path}/bin:{current_path}",
    }
    logger.info(f"running {args}")
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            input=input_,
            encoding="utf-8",
            env=ccp4_env,
        )
        if result.returncode != 0:
            logger.exception(
                f"calling {args} didn't work: {result.stderr}, stderr: {result.stdout}"
            )
            raise Exception()
        return result.stdout
    except:
        logger.exception(f"calling {args} didn't work")
        raise Exception()


def extract_labels_from_mtzinfo(mtzinfo_output: str) -> list[str]:
    input_mtz_labels_lines = [
        line for line in mtzinfo_output.split("\n") if line.startswith("LABELS ")
    ]
    if not input_mtz_labels_lines:
        raise Exception("couldn't parse mtzinfo output, no line starting with LABELS!")
    return input_mtz_labels_lines[0].split(" ")[1:]


def uniqify(
    ccp4_path: Path, rfree_mtz: Path, input_mtz: Path, resolution_cut: float
) -> Path:
    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/pointless",
            "hklref",
            str(rfree_mtz),
            "hklin",
            str(input_mtz),
            "hklout",
            POINTLESS_MTZ,
        ],
    )

    mtzinfo_input_mtz = ccp4_run(ccp4_path, [f"{ccp4_path}/bin/mtzinfo", POINTLESS_MTZ])

    input_mtz_labels = extract_labels_from_mtzinfo(mtzinfo_input_mtz)

    input_mtz_rfree_labels = [
        label for label in input_mtz_labels if "free" in label.lower()
    ]
    if len(input_mtz_rfree_labels) > 1:
        raise Exception(
            f'couldn\'t do refinement, there is more than one "free" in "{POINTLESS_MTZ}": '
            + ", ".join(input_mtz_rfree_labels)
        )
    if input_mtz_rfree_labels:
        logger.info(
            f'Input MTZ contains rfree flags in column "{input_mtz_rfree_labels[0]}", excluding those using mtzutils call'
        )
        ccp4_run(
            ccp4_path,
            [
                f"{ccp4_path}/bin/mtzutils",
                "hklin",
                POINTLESS_MTZ,
                "hklout",
                EXCLUSION_MTZ,
            ],
            input_=f"exclude {input_mtz_rfree_labels[0]}",
        )
    else:
        logger.info(
            f"Input MTZ doesn't contain RFree flags column (columns are {input_mtz_labels}), just copying"
        )
        shutil.copyfile(POINTLESS_MTZ, EXCLUSION_MTZ)
    xdata_lines = [
        line for line in mtzinfo_input_mtz.split("\n") if line.startswith("XDATA ")
    ]
    if not xdata_lines:
        raise Exception("couldn't parse mtzinfo output, no line starting with XDATA!")
    xdata_line = re.split(r" +", xdata_lines[0])
    logger.info(f"xdata line is {xdata_lines[0]} ({len(xdata_line)} component(s))")

    mtzinfo_rfree_mtz = ccp4_run(
        ccp4_path, [f"{ccp4_path}/bin/mtzinfo", str(rfree_mtz)]
    )
    rfree_mtz_labels = extract_labels_from_mtzinfo(mtzinfo_rfree_mtz)
    rfree_mtz_rfree_labels = [
        label for label in rfree_mtz_labels if "free" in label.lower()
    ]
    if not rfree_mtz_rfree_labels:
        raise Exception(
            f'couldn\'t find a "free" column in "{rfree_mtz}", columns are: '
            + ",".join(rfree_mtz_labels)
        )
    rfree_mtz_column = rfree_mtz_rfree_labels[0]
    logger.info(f"using column {rfree_mtz_column} as free flag in {rfree_mtz}")

    unique_cell_information = f"CELL {xdata_line[1]} {xdata_line[2]} {xdata_line[3]} {xdata_line[4]} {xdata_line[5]} {xdata_line[6]} {xdata_line[7]} SYMMETRY {xdata_line[9]}"
    logger.info(f"unique cell information: {unique_cell_information}")
    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/unique",
            "HKLOUT",
            UNIQUE_MTZ,
        ],
        input_=f"""{unique_cell_information}
    LABOUT F=FUNI SIGF=SIGFUNI
    RESOLUTION {xdata_line[8]}
    SYMM {xdata_line[9]}""",
    )

    cad_input = f"""
    LABIN FILE 1  ALLIN
    LABIN FILE 2  ALLIN
    LABIN FILE 3 E1 = {rfree_mtz_column}
    """
    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/cad",
            "HKLIN1",
            EXCLUSION_MTZ,
            "HKLIN2",
            UNIQUE_MTZ,
            "HKLIN3",
            str(rfree_mtz),
            "HKLOUT",
            CAD_MTZ,
        ],
        input_=cad_input,
    )

    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/freerflag",
            "HKLIN",
            CAD_MTZ,
            "HKLOUT",
            FREER_MTZ,
        ],
        input_=f"""
    COMPLETE FREE={rfree_mtz_column}
            """,
    )

    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/mtzutils",
            "hklin",
            FREER_MTZ,
            "hklout",
            UNIQIFIED_MTZ,
        ],
        input_=f"""
    EXCLUDE FUNI SIGFUNI
    SYMM {xdata_line[9]}
            """,
    )

    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/mtzutils",
            "hklin",
            UNIQIFIED_MTZ,
            "hklout",
            RESCUT_MTZ,
        ],
        input_=f"""
    resolution {resolution_cut}
            """,
    )

    return Path(UNIQIFIED_MTZ)


@dataclass(frozen=True)
class RefinementFom:
    r_free: float
    r_work: float
    rms_bond_angle: float
    rms_bond_length: float


@dataclass(frozen=True)
class RefinementResult:
    pdb_path: Path
    mtz_path: Path
    fom: RefinementFom


def parse_refmac_log(p: Path) -> RefinementFom:
    r_work: None | float = None
    r_free: None | float = None
    rms_bond_length: None | float = None
    rms_bond_angle: None | float = None
    R_WORK_REGEX = re.compile(r"R factor\s+(\S+)\s+(\S+)")
    R_FREE_REGEX = re.compile(r"R free\s+(\S+)\s+(\S+)")
    RMS_BOND_ANGLE_REGEX = re.compile(r"Rms BondAngle\s+(\S+)\s+(\S+)")
    RMS_BOND_LENGTH_REGEX = re.compile(r"Rms BondLength\s+(\S+)\s+(\S+)")

    def extract_final_result(
        regex: re.Pattern[str], this_line: str, previous_result: None | float
    ) -> None | float:
        regex_result = regex.search(this_line)
        if regex_result is not None:
            try:
                return float(regex_result.group(2))
            except:
                return previous_result
        return previous_result

    with p.open("r") as f:
        for line in f:
            r_work = extract_final_result(R_WORK_REGEX, line, r_work)
            r_free = extract_final_result(R_FREE_REGEX, line, r_free)
            rms_bond_angle = extract_final_result(
                RMS_BOND_ANGLE_REGEX, line, rms_bond_angle
            )
            rms_bond_length = extract_final_result(
                RMS_BOND_LENGTH_REGEX, line, rms_bond_length
            )

    if (
        r_work is not None
        and r_free is not None
        and rms_bond_length is not None
        and rms_bond_angle is not None
    ):
        return RefinementFom(
            r_work=r_work,
            r_free=r_free,
            rms_bond_angle=rms_bond_angle,
            rms_bond_length=rms_bond_length,
        )

    raise Exception(
        f"not all of the figures of merit are given: Rwork={r_work}, Rfree={r_free}, RMS bond length={rms_bond_length}, RMS bond angle={rms_bond_angle}"
    )


def quick_refine(
    ccp4_path: Path,
    input_mtz: Path,
    resolution_cut: float,
    input_pdb: Path,
    input_restraints_cif: None | Path,
) -> RefinementResult:
    logger.info("cutting resolution...")

    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/mtzutils",
            "hklin",
            str(input_mtz),
            "hklout",
            RESCUT_MTZ,
        ],
        input_=f"""
    resolution {resolution_cut}
            """,
    )

    logging.info("running dimple now")

    ccp4_run(
        ccp4_path,
        [
            f"{ccp4_path}/bin/dimple",
            "-f",
            "png",
            "--jelly",
            "0",
            "--restr-cycles",
            "15",
            "--hklout",
            DIMPLE_OUT_MTZ,
            "--xyzout",
            DIMPLE_OUT_PDB,
        ]
        + (
            ["--libin", str(input_restraints_cif)]
            if input_restraints_cif is not None
            else []
        )
        + [
            RESCUT_MTZ,
            str(input_pdb),
            ".",
        ],
    )

    REFMAC_LOG_GLOB = "*refmac5_restr*.log"
    refmac_log_files = list(Path("./").glob(REFMAC_LOG_GLOB))

    if not refmac_log_files:
        error = f"dimple ran successfully, but didn't produce file matching {REFMAC_LOG_GLOB}, please check the output"
        raise Exception(error)

    if len(refmac_log_files) > 1:
        logging.info(
            f"found multiple refmac log files: {refmac_log_files}, taking the first one"
        )

    return RefinementResult(
        pdb_path=Path(DIMPLE_OUT_PDB),
        mtz_path=Path(DIMPLE_OUT_MTZ),
        fom=parse_refmac_log(refmac_log_files[0]),
    )


logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


@dataclass(frozen=True)
class ParsedArgs:
    stream_files: list[Path]
    api_url: str
    merge_result_id: int
    cell_file_id: int
    point_group: str
    hkl_file: Path
    ccp4_path: None | Path
    partialator_additional: None | str
    crystfel_path: Path
    pdb_file_id: None | int
    restraints_cif_file_id: None | int
    random_cut_length: None | int


def parse_predefined(s: bytes) -> ParsedArgs:
    j = json.loads(b64decode(s))
    assert isinstance(j, dict)
    crystfel_path_str = j.get("crystfel-path")
    if crystfel_path_str is None:
        exit_with_error(None, "crystfel-path missing in input")
    if not isinstance(crystfel_path_str, str):
        exit_with_error(None, f"crystfel-path not a string but {crystfel_path_str}")
    crystfel_path = Path(crystfel_path_str)
    if not crystfel_path.is_dir():
        exit_with_error(
            None, f"CrystFEL path {crystfel_path} must be a valid directory"
        )

    ccp4_path_str = j.get("ccp4-path")
    if ccp4_path_str is None:
        exit_with_error(None, "ccp4-path missing in input")
    if not isinstance(ccp4_path_str, str):
        exit_with_error(None, f"ccp4-path not a string but {ccp4_path_str}")
    ccp4_path = Path(ccp4_path_str) if ccp4_path_str else None
    if ccp4_path and not ccp4_path.is_dir():
        exit_with_error(
            None, f"ccp4 path {ccp4_path} must be a valid directory or empty"
        )

    stream_files_raw = j.get("stream-files")
    if stream_files_raw is None:
        exit_with_error(None, "stream-files missing in input")
    if not isinstance(stream_files_raw, list):
        exit_with_error(None, f"stream-files not a list but {stream_files_raw}")
    for idx, sf in enumerate(
        stream_files_raw  # pyright: ignore [reportUnknownArgumentType]
    ):
        if not isinstance(sf, str):
            exit_with_error(None, f"stream-files[{idx}] not a string but {sf}")
    stream_files = [
        Path(p) for p in stream_files_raw  # pyright: ignore [reportUnknownArgumentType]
    ]
    if not stream_files:
        exit_with_error(None, "no input stream files given")

    invalid_paths = set(f for f in stream_files if not f.is_file())
    if invalid_paths:
        logger.warning(
            "the following file(s) are not valid stream files: "
            + ", ".join(str(f) for f in stream_files if not f.is_file())
        )
        if invalid_paths == set(stream_files):
            exit_with_error(None, "none of the input stream files is a valid file")
    valid_paths = [f for f in stream_files if f.is_file()]
    return ParsedArgs(
        stream_files=valid_paths,
        api_url=j.get("api-url"),  # type: ignore
        merge_result_id=j.get("merge-result-id"),  # type: ignore
        cell_file_id=j.get("cell-file-id"),  # type: ignore
        point_group=j.get("point-group"),  # type: ignore
        ccp4_path=ccp4_path,
        hkl_file=Path(
            j.get(
                "hkl-file", "partialator.hkl"
            )  # pyright: ignore [reportUnknownArgumentType]
        ),
        partialator_additional=j.get(
            "partialator-additional"
        ),  # pyright: ignore [reportUnknownArgumentType]
        crystfel_path=crystfel_path,
        pdb_file_id=j.get("pdb-file-id"),  # pyright: ignore [reportUnknownArgumentType]
        restraints_cif_file_id=j.get(
            "restraints-cif-file-id"
        ),  # pyright: ignore [reportUnknownArgumentType]
        random_cut_length=j.get("random-cut-length"),  # type: ignore
    )


predefined_args: None | bytes = None


def retrieve_file(args: ParsedArgs, file_id: int, name: str) -> Path:
    url = f"{args.api_url}/api/files/{file_id}"
    req = request.Request(url, method="GET")
    logger.info(f"requesting file on {url}")
    with request.urlopen(req) as response:
        with Path(name).open("wb") as output_file:
            output_file.write(response.read())
    return Path(name)


def upload_file(args: ParsedArgs, file_path: Path) -> int:
    with file_path.open("rb") as file_obj:
        url = f"{args.api_url}/api/files/simple/{file_path.suffix.replace('.', '')}"
        logger.info(f"uploading file to {url}")
        req = request.Request(
            url,
            method="POST",
            data=file_obj,
        )

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
            exit_with_error(args, f"error uploading file {file_path}")


def write_output_json(
    args: ParsedArgs, error: None | str, result: None | dict[str, Any]
) -> None:
    try:
        result_json = json.dumps(
            {"error": error, "result": result}, allow_nan=False, indent=2
        ).encode("utf-8")
    except ValueError:
        result_json_with_nan = json.dumps(
            {"error": error, "result": result}, allow_nan=True, indent=2
        ).encode("utf-8")
        logger.info(
            f"couldn't serialize output json - it probably contained NaN: {result_json_with_nan}"
        )
        result_json = json.dumps(
            {
                "error": 'The merge result contained invalid statistics (probably CC* is "not a number"). Try collecting more data, using different merge parameters or indexing prior runs manually to fix this. The full output of this merge job contains the final JSON with the invalid stats, you can take a look if you have access to it.',
                "result": None,
            },
            allow_nan=False,
            indent=2,
        ).encode("utf-8")

    url = f"{args.api_url}/api/merging/{args.merge_result_id}/finish"
    logger.info(f"sending result to {url}")
    req = request.Request(
        url,
        data=result_json,
        method="POST",
    )
    req.add_header("Content-Type", "application/json")
    with request.urlopen(req) as response:
        logger.info(f"received the following response from server: {response.read()}")


def exit_with_error(args: None | ParsedArgs, message: str) -> NoReturn:
    logger.error(message)
    if args is not None:
        write_output_json(args, error=message, result=None)
    sys.exit(1)


def first_group(output: str, regex: str) -> str:
    reg = re.compile(regex, re.MULTILINE).search(output)
    if reg is None:
        raise Exception(
            f"regular expression...\n\n{regex}\n\n...matches nowhere in output:\n\n{output}"
        )
    return reg.group(1)


def first_group_as_float(output: str, regex: str) -> float:
    result = first_group(output, regex)
    try:
        return float(result)
    except:
        raise Exception(
            f'regular expression "{regex}" doesn\'t match a float value but {result}'
        )


def first_group_as_int(output: str, regex: str) -> int:
    result = first_group(output, regex)
    try:
        return int(result)
    except:
        raise Exception(
            f'regular expression "{regex}" doesn\'t match an int value but {result}'
        )


@dataclass(frozen=True)
class CheckHklArgs:
    crystfel_path: Path
    hkl_file: Path
    point_group: str
    unit_cell: Path
    ltest: None | bool = None
    wilson: None | bool = None
    sigma_cutoff: None | float = None
    nshells: None | int = None
    rmin: None | float = None
    rmax: None | float = None
    lowres: None | float = None
    highres: None | float = None
    shell_file: None | Path = None


@dataclass(frozen=True)
class CompareHklArgs:
    crystfel_path: Path
    hkl1: Path
    hkl2: Path
    point_group: str
    unit_cell: Path
    fom: None | str = None
    nshells: None | int = None
    shell_file: None | Path = None
    scale_to_unity: None | bool = None
    sigma_cutoff: None | float = None
    rmin: None | float = None
    rmax: None | float = None
    lowres: None | float = None
    highres: None | float = None


def compare_hkl_args_to_list(args: CompareHklArgs) -> list[str]:
    cli_args = [
        f"{args.crystfel_path}/bin/compare_hkl",
        str(args.hkl1),
        str(args.hkl2),
        "-y",
        args.point_group,
        "-p",
        str(args.unit_cell),
    ]
    if args.fom is not None:
        cli_args.append(f"--fom={args.fom}")
    if args.nshells is not None:
        cli_args.append(f"--nshells={args.nshells}")
    if args.shell_file is not None:
        cli_args.append(f"--shell-file={args.shell_file}")
    if args.scale_to_unity is not None and args.scale_to_unity:
        cli_args.append("-u")
    if args.sigma_cutoff is not None:
        cli_args.append(f"--sigma=cutoff={args.sigma_cutoff}")
    if args.rmin is not None:
        cli_args.append(f"--rmin={args.rmin}")
    if args.rmax is not None:
        cli_args.append(f"--rmax={args.rmax}")
    if args.lowres is not None:
        cli_args.append(f"--lowres={args.lowres}")
    if args.highres is not None:
        cli_args.append(f"--highres={args.highres}")
    return cli_args


def check_hkl_args_to_list(args: CheckHklArgs) -> list[str]:
    cli_args = [
        f"{args.crystfel_path}/bin/check_hkl",
        str(args.hkl_file),
        "-y",
        args.point_group,
        "-p",
        str(args.unit_cell),
    ]
    if args.nshells is not None:
        cli_args.append(f"--nshells={args.nshells}")
    if args.shell_file is not None:
        cli_args.append(f"--shell-file={args.shell_file}")
    if args.ltest is not None and args.ltest:
        cli_args.append("--ltest")
    if args.wilson is not None and args.wilson:
        cli_args.append("--wilson")
    if args.sigma_cutoff is not None:
        cli_args.append(f"--sigma=cutoff={args.sigma_cutoff}")
    if args.rmin is not None:
        cli_args.append(f"--rmin={args.rmin}")
    if args.rmax is not None:
        cli_args.append(f"--rmax={args.rmax}")
    if args.lowres is not None:
        cli_args.append(f"--lowres={args.lowres}")
    if args.highres is not None:
        cli_args.append(f"--highres={args.highres}")
    return cli_args


def run_compare_hkl_single_fom(
    args: ParsedArgs,
    fom: str,
    search_term: str,
    highres: None | float,
    nshells: int,
    may_fail: bool = False,
    output_file: None | Path = None,
) -> None | float:
    compare_hkl_command_line_args = compare_hkl_args_to_list(
        CompareHklArgs(
            crystfel_path=args.crystfel_path,
            point_group=args.point_group,
            unit_cell=retrieve_file(args, args.cell_file_id, "cell"),
            hkl1=args.hkl_file.with_suffix(".hkl1"),
            hkl2=args.hkl_file.with_suffix(".hkl2"),
            fom=fom,
            shell_file=output_file,
            highres=highres,
            nshells=nshells,
        )
    )
    logging.info(
        f"starting compare_hkl with command line: {compare_hkl_command_line_args}"
    )
    try:
        compare_hkl_result = subprocess.run(
            compare_hkl_command_line_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )

        if compare_hkl_result.returncode != 0:
            if may_fail:
                return None
            exit_with_error(
                args,
                f"error running compare_hkl, error code is {compare_hkl_result.returncode}, output:\n\n{compare_hkl_result.stdout}",
            )

        return first_group_as_float(
            compare_hkl_result.stdout,
            rf"{re.escape(search_term)} = ([^\n% ]+)",
        )
    except:
        if may_fail:
            return None
        exit_with_error(args, "error running compare_hkl")


@dataclass(frozen=True)
class CheckShellLine:
    center_1_nm: float
    nref: int
    possible: int
    compl: float
    meas: int
    red: float
    snr: float
    mean_i: float
    d_a: float
    min_1_nm: float
    max_1_nm: float


def read_shells_file(args: ParsedArgs, file_path: Path) -> list[CheckShellLine]:
    with file_path.open("r", encoding="utf-8") as shells_file:
        result: list[CheckShellLine] = []
        # Ignore header line (we cannot separate by whitespace here)
        shells_file.readline()
        for line_no, line in enumerate(shells_file, start=2):
            split_line = line.split()
            if len(split_line) != _NUMBER_OF_COLUMNS_IN_CHECK_SHELL_FILE:
                exit_with_error(
                    args,
                    f"couldn't read file {file_path}, line {line_no} has invalid format (number of columns not {_NUMBER_OF_COLUMNS_IN_CHECK_SHELL_FILE}): {line}",
                )
            try:
                result.append(
                    CheckShellLine(
                        center_1_nm=float(split_line[0]),
                        nref=int(split_line[1]),
                        possible=int(split_line[2]),
                        compl=float(split_line[3]),
                        meas=int(split_line[4]),
                        red=float(split_line[5]),
                        snr=float(split_line[6]),
                        mean_i=float(split_line[7]),
                        d_a=float(split_line[8]),
                        min_1_nm=float(split_line[9]),
                        max_1_nm=float(split_line[10]),
                    )
                )
            except:
                exit_with_error(
                    args,
                    f"couldn't read file {file_path}, line {line_no} has invalid format: {line}",
                )
        return result


@dataclass(frozen=True)
class CompareShellLine:
    one_over_d_centre: float
    fom_value: float
    nref: int
    d_over_a: float
    min_1_nm: float
    max_1_nm: float


def read_compare_shells_file(
    args: ParsedArgs, file_path: Path
) -> list[CompareShellLine]:
    with file_path.open("r", encoding="utf-8") as shells_file:
        result: list[CompareShellLine] = []
        # Ignore header line (we cannot separate by whitespace here)
        shells_file.readline()
        for line_no, line in enumerate(shells_file, start=2):
            split_line = line.split()
            if len(split_line) != _NUMBER_OF_COLUMNS_IN_COMPARE_SHELL_FILE:
                exit_with_error(
                    args,
                    f"couldn't read file {file_path}, line {line_no} has invalid format (number of columns not {_NUMBER_OF_COLUMNS_IN_COMPARE_SHELL_FILE}): {line}",
                )
            try:
                result.append(
                    CompareShellLine(
                        one_over_d_centre=float(split_line[0]),
                        fom_value=float(split_line[1]),
                        nref=int(split_line[2]),
                        d_over_a=float(split_line[3]),
                        min_1_nm=float(split_line[4]),
                        max_1_nm=float(split_line[5]),
                    )
                )
            except:
                exit_with_error(
                    args,
                    f"couldn't read file {file_path}, line {line_no} has invalid format: {line}",
                )
        return result


def run_check_hkl(args: ParsedArgs, check_hkl_args: CheckHklArgs) -> str:
    check_hkl_command_line_args = check_hkl_args_to_list(check_hkl_args)
    logging.info(f"starting check_hkl with command line: {check_hkl_command_line_args}")
    try:
        check_hkl_result = subprocess.run(
            check_hkl_command_line_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )

        if check_hkl_result.returncode != 0:
            exit_with_error(
                args,
                f"error running check_hkl, error code is {check_hkl_result.returncode}",
            )

        return check_hkl_result.stdout
    except:
        exit_with_error(args, "error running check_hkl")


def create_mtz(args: ParsedArgs, output_path: Path, cell_file: Path) -> None:
    try:
        cli_args = [
            f"{args.crystfel_path}/bin/get_hkl",
            "-i",
            str(args.hkl_file),
            "-p",
            str(cell_file),
            "-o",
            str(output_path),
            "--output-format=mtz",
        ]
        logging.info(f"starting get_hkl with command line: {cli_args}")
        result = subprocess.run(
            cli_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
        )

        if result.returncode != 0:
            exit_with_error(
                args, f"error running get_hkl, error code is {result.returncode}"
            )
    except:
        exit_with_error(args, "error running get_hkl")


@dataclass(frozen=True)
class Chunk:
    file: Path
    start: int
    length: int
    indexed_by: str


_INDEXED_BY_PREFIX: Final = "indexed_by = "


def read_chunks(files: Iterable[Path]) -> Generator[Chunk, None, None]:
    for p in files:
        with p.open("r", encoding="utf-8") as f:
            start: None | int = None
            indexed_by: None | str = None
            line_number = 0
            # see
            # https://stackoverflow.com/questions/29618936/how-to-solve-oserror-telling-position-disabled-by-next-call
            while line := f.readline():
                line_number += 1
                if line.startswith("----- Begin chunk"):
                    # This wrongfully assumes characters are always 1 byte long.
                    start = f.tell() - len(line)
                    indexed_by = None
                elif line.startswith("----- End chunk"):
                    end = f.tell()

                    if indexed_by is None:
                        logger.warning(
                            f"{p}:{line_number}: chunk without indexed_by, start {start}, end {end}: {line}"
                        )
                    elif start is None:
                        logger.warning(
                            f"{p}:{line_number}: chunk end without start, end {end}: {line}"
                        )
                    else:
                        yield Chunk(
                            file=p,
                            start=start,
                            length=end - start,
                            indexed_by=indexed_by,
                        )
                        start = None
                        indexed_by = None
                elif line.startswith(_INDEXED_BY_PREFIX):
                    if start is None:
                        logger.warning(
                            f"{p}:{line_number}: indexed_by without chunk found: {line}"
                        )
                    else:
                        indexed_by = line[len(_INDEXED_BY_PREFIX) :].strip()


T = TypeVar("T")


def reservoir_sample(xs: Iterable[T], max_items: int, rng_seed: int) -> list[T]:
    rng = Random(rng_seed)

    result: list[T] = []
    for x in xs:
        chunk_len = len(result)
        if chunk_len < max_items:
            result.append(x)
        else:
            j = rng.randint(1, chunk_len)
            if j <= max_items:
                result[j - 1] = x

    return result


def write_random_chunks(
    file_list: list[Path], max_chunks: int, target: BinaryIO
) -> None:
    current_file_obj: None | BinaryIO = None
    try:
        current_file_path: None | Path = None

        for chunk in reservoir_sample(
            (x for x in read_chunks(file_list) if x.indexed_by != "none"),
            max_chunks,
            sum(
                bytearray(
                    "".join(str(single_file) for single_file in file_list),
                    encoding="utf-8",
                )
            ),
        ):
            if chunk.file != current_file_path:
                if current_file_obj is not None:
                    current_file_obj.close()
                current_file_obj = chunk.file.open("rb")
                current_file_path = chunk.file

            assert current_file_obj is not None

            current_file_obj.seek(chunk.start)
            target.write(current_file_obj.read(chunk.length))
    finally:
        if current_file_obj is not None:
            current_file_obj.close()


def generate_output(args: ParsedArgs) -> None:
    merge_subdirectory = Path(f"./merging-{args.merge_result_id}")

    merge_subdirectory.mkdir(parents=True, exist_ok=True)
    os.chdir(merge_subdirectory)

    if args.random_cut_length is not None:
        with NamedTemporaryFile(dir=os.getcwd()) as random_chunks_file:
            with args.stream_files[0].open("r", encoding="utf-8") as first_file:
                header = ""
                for line in first_file:
                    if line.startswith("----- Begin chunk"):
                        break
                    header += line

            random_chunks_file.write(header.encode("utf-8"))
            write_random_chunks(
                file_list=args.stream_files,
                max_chunks=args.random_cut_length,
                # How do you type the output of NamedTemporaryFile? It's obviously a binary file object
                # in the default mode.
                target=random_chunks_file,  # type: ignore
            )
            random_chunks_file.flush()
            run_partialator(args, Path(random_chunks_file.name))
    else:
        run_partialator(args, None)

    cell_file = retrieve_file(args, args.cell_file_id, "cell")

    mtz_path = Path(f"output-{args.merge_result_id}.mtz")
    create_mtz(args, mtz_path, cell_file)

    highres_cut, nshells = calculate_highres_cut(args)

    logger.info(f"highres cut is {highres_cut}, {nshells} shell(s)")

    check_out = run_check_hkl(
        args,
        CheckHklArgs(
            crystfel_path=args.crystfel_path,
            hkl_file=args.hkl_file,
            point_group=args.point_group,
            unit_cell=cell_file,
            highres=highres_cut,
            shell_file=CHECK_HKL_SHELL_FILE,
            nshells=nshells,
        ),
    )
    snr = first_group_as_float(check_out, r"Overall <snr> = ([^\n]+)")
    redundancy = first_group_as_float(
        check_out,
        r"Overall redundancy = ([0-9.]+) measurements/unique reflection",
    )
    completeness = first_group_as_float(
        check_out, r"Overall completeness = ([0-9.]+) %"
    )
    measurements_total = first_group_as_int(
        check_out, r"([0-9]+) measurements in total"
    )
    reflections_total = first_group_as_int(check_out, r"([0-9]+) reflections in total")
    reflections_possible = first_group_as_int(
        check_out, r"([0-9]+) reflections possible"
    )
    discarded_reflections = first_group_as_int(
        check_out, r"Discarded ([0-9]+) reflections"
    )
    one_over_d = re.compile(r"1/d goes from ([0-9.]+) to ([0-9.]+) nm\^-1").search(
        check_out, re.MULTILINE
    )
    if one_over_d is None:
        raise Exception(
            f'couldn\'t find the line starting with "1/d goes from" in\n\n{check_out}'
        )
    one_over_d_from = 10.0 / float(one_over_d.group(1))
    one_over_d_to = 10.0 / float(one_over_d.group(2))

    wilson_out = run_check_hkl(
        args,
        CheckHklArgs(
            hkl_file=args.hkl_file,
            crystfel_path=args.crystfel_path,
            point_group=args.point_group,
            unit_cell=cell_file,
            highres=highres_cut,
            wilson=True,
            nshells=nshells,
        ),
    )
    try:
        wilson = first_group_as_float(wilson_out, r"B = ([^ ]+)")
        ln_k = first_group_as_float(wilson_out, r"ln k = ([^\n]+)")
    except:
        wilson = None
        ln_k = None

    rsplit = run_compare_hkl_single_fom(
        args,
        "Rsplit",
        "Overall Rsplit",
        highres_cut,
        nshells=nshells,
        output_file=RSPLIT_COMPARE_SHELL_FILE,
    )
    ccstar = run_compare_hkl_single_fom(
        args,
        "CCstar",
        "Overall CC*",
        highres_cut,
        nshells=nshells,
        output_file=CCSTAR_COMPARE_SHELL_FILE,
    )
    cc = run_compare_hkl_single_fom(
        args,
        "CC",
        "Overall CC",
        highres_cut,
        nshells=nshells,
        output_file=CC_COMPARE_SHELL_FILE,
    )
    check_file = read_shells_file(args, CHECK_HKL_SHELL_FILE)
    if not check_file:
        exit_with_error(
            args, f"cannot proceed, check file {CHECK_HKL_SHELL_FILE} has no lines"
        )

    refinement_result: None | RefinementResult = None
    if args.pdb_file_id is not None and args.ccp4_path is not None:
        try:
            pdb_file = retrieve_file(args, args.pdb_file_id, "base-model.pdb")
            restraints_cif_file = (
                retrieve_file(args, args.restraints_cif_file_id, "restraints.cif")
                if args.restraints_cif_file_id is not None
                else None
            )
            refinement_result = quick_refine(
                args.ccp4_path, mtz_path, highres_cut, pdb_file, restraints_cif_file
            )
        except:
            logger.exception("couldn't complete refinement")

    output_json = {
        "mtz_file_id": upload_file(args, mtz_path),
        "detailed_foms": extract_shell_resolutions(args),
        "refinement_results": (
            [
                {
                    "pdb_file_id": upload_file(args, refinement_result.pdb_path),
                    "mtz_file_id": upload_file(args, refinement_result.mtz_path),
                    "r_free": refinement_result.fom.r_free,
                    "r_work": refinement_result.fom.r_work,
                    "rms_bond_angle": refinement_result.fom.rms_bond_angle,
                    "rms_bond_length": refinement_result.fom.rms_bond_length,
                }
            ]
            if refinement_result is not None
            else []
        ),
        "fom": {
            "snr": snr,
            "wilson": (
                None if wilson is None else None if math.isnan(wilson) else wilson
            ),
            "ln_k": None if ln_k is None else None if math.isnan(ln_k) else ln_k,
            "discarded_reflections": discarded_reflections,
            "one_over_d_from": one_over_d_from,
            "one_over_d_to": one_over_d_to,
            "redundancy": redundancy,
            "completeness": completeness,
            "measurements_total": measurements_total,
            "reflections_total": reflections_total,
            "reflections_possible": reflections_possible,
            "r_split": rsplit,
            "r1i": run_compare_hkl_single_fom(
                args,
                "R1i",
                "Overall R1(I)",
                highres_cut,
                nshells=nshells,
            ),
            "r2": run_compare_hkl_single_fom(
                args,
                "R2",
                "Overall R(2)",
                highres_cut,
                nshells=nshells,
            ),
            "cc": cc,
            "ccstar": ccstar,
            "ccano": run_compare_hkl_single_fom(
                args,
                "CC",
                "Overall CCano",
                highres_cut,
                nshells=nshells,
                may_fail=True,
            ),
            "crdano": run_compare_hkl_single_fom(
                args,
                "CRDano",
                "Overall CRDano",
                highres_cut,
                nshells=nshells,
                may_fail=True,
            ),
            "rano": run_compare_hkl_single_fom(
                args,
                "Rano",
                "Overall Rano",
                highres_cut,
                nshells=nshells,
                may_fail=True,
            ),
            "rano_over_r_split": run_compare_hkl_single_fom(
                args,
                "Rano/Rsplit",
                "Overall Rano/Rsplit",
                highres_cut,
                nshells=nshells,
                may_fail=True,
            ),
            "d1sig": run_compare_hkl_single_fom(
                args,
                "d1sig",
                "Fraction of differences less than 1 sigma",
                highres_cut,
                nshells=nshells,
            ),
            "d2sig": run_compare_hkl_single_fom(
                args,
                "d2sig",
                "Fraction of differences less than 2 sigma",
                highres_cut,
                nshells=nshells,
            ),
            "outer_shell": {
                "resolution": read_compare_shells_file(
                    args, ccstar_compare_shell_file(nshells)
                )[-1].d_over_a,
                "ccstar": read_compare_shells_file(args, CCSTAR_COMPARE_SHELL_FILE)[
                    -1
                ].fom_value,
                "r_split": read_compare_shells_file(args, RSPLIT_COMPARE_SHELL_FILE)[
                    -1
                ].fom_value,
                "cc": read_compare_shells_file(args, CC_COMPARE_SHELL_FILE)[
                    -1
                ].fom_value,
                "unique_reflections": check_file[-1].nref,
                "completeness": check_file[-1].compl,
                "redundancy": check_file[-1].red,
                "snr": check_file[-1].snr,
                "min_res": 10.0 / check_file[-1].min_1_nm,
                "max_res": 10.0 / check_file[-1].max_1_nm,
            },
        },
    }

    write_output_json(args, error=None, result=output_json)


def extract_shell_resolutions(args: ParsedArgs) -> list[dict[str, float | int]]:
    return [
        {
            "one_over_d_centre": ccstar.one_over_d_centre,
            "nref": ccstar.nref,
            "d_over_a": ccstar.d_over_a,
            "min_res": 10.0 / ccstar.min_1_nm,
            "max_res": 10.0 / ccstar.max_1_nm,
            "cc": cc.fom_value,
            "ccstar": ccstar.fom_value,
            "r_split": rsplit.fom_value,
            "reflections_possible": checkhkl.possible,
            "completeness": checkhkl.compl,
            "measurements": checkhkl.meas,
            "redundancy": checkhkl.red,
            "snr": checkhkl.snr,
            "mean_i": checkhkl.mean_i,
        }
        for ccstar, cc, rsplit, checkhkl in zip(
            read_compare_shells_file(args, CCSTAR_COMPARE_SHELL_FILE),
            read_compare_shells_file(args, CC_COMPARE_SHELL_FILE),
            read_compare_shells_file(args, RSPLIT_COMPARE_SHELL_FILE),
            read_shells_file(args, CHECK_HKL_SHELL_FILE),
        )
    ]


def calculate_highres_cut(args: ParsedArgs) -> tuple[float, int]:
    def calculate_ccstar_values(nshells: int) -> None | tuple[float, int]:
        output_file = ccstar_compare_shell_file(nshells)
        run_compare_hkl_single_fom(
            args,
            "CCstar",
            "Overall CC*",
            output_file=output_file,
            highres=None,
            nshells=nshells,
        )
        first_pass_ccstar_file = read_compare_shells_file(args, output_file)
        if not first_pass_ccstar_file:
            logger.warning(
                f"Error in data: CC* shells file for {nshells} shell(s), cannot calculate cutoff - continuing with more shells"
            )
            return None
        highres_cut_line: None | CompareShellLine = None
        for line in first_pass_ccstar_file:
            if math.isnan(line.fom_value):
                continue
            if line.fom_value < _HIGHRES_CUT_CCSTAR_THRESHOLD:
                break
            highres_cut_line = line
        if highres_cut_line is None:
            highres_cut_line = first_pass_ccstar_file[-1]
        return highres_cut_line.d_over_a, min(x.nref for x in first_pass_ccstar_file)

    reasonable_nshell = 20
    highres_cut_and_minimum_nref = calculate_ccstar_values(reasonable_nshell)
    if highres_cut_and_minimum_nref is None:
        exit_with_error(
            args, f"Error in data: CC* shells file for {reasonable_nshell} shell(s)"
        )
    highres_cut, _ = highres_cut_and_minimum_nref
    return highres_cut, reasonable_nshell
    # result: None | tuple[float, int] = None
    # for nshells in range(1, _MAX_SHELLS_TO_TEST):
    #     highres_cut_and_minimum_nref = calculate_ccstar_values(nshells)
    #     if highres_cut_and_minimum_nref is None:
    #         logger.warning(
    #             f"Error in data: CC* shells file for {nshells} shell(s), cannot calculate cutoff - continuing with more shells"
    #         )
    #         continue
    #     highres_cut, minimum_nref = highres_cut_and_minimum_nref
    #     if minimum_nref > DESIRED_NREFS_PER_SHELL:
    #         result = highres_cut, nshells
    #     else:
    #         if result is None:
    #             logger.warning(
    #                 f"after {nshells} shell(s), we have shells with less than {DESIRED_NREFS_PER_SHELL} refs, but we found no number of shells that match, so taking this one"
    #             )
    #             return highres_cut, nshells
    #         return result
    # exit_with_error(
    #     args,
    #     f"considered all number of shells from 1 to {_MAX_SHELLS_TO_TEST}, but found no good configuration",
    # )


def run_partialator(args: ParsedArgs, random_cut_file: None | Path) -> None:
    partialator_command_line_args = [
        f"{args.crystfel_path}/bin/partialator",
        "-y",
        args.point_group,
        "-j",
        str(multiprocessing.cpu_count()),
        "-o",
        str(args.hkl_file),
    ]
    if args.partialator_additional:
        partialator_command_line_args.extend(shlex.split(args.partialator_additional))
    # This is implemented a little lazily, to be honest. If we have a "random cut file", meaning we stitched
    # together stream fils into a temporary new stream file, then run partialator on that
    # otherwise run it on the input stream files.
    #
    # This is lazy, because this file choosing should really be in this funciton, rather than the one on top of
    # it. But there you go.
    if random_cut_file is not None:
        partialator_command_line_args.extend(["-i", str(random_cut_file)])
    else:
        for f in args.stream_files:
            partialator_command_line_args.extend(["-i", str(f)])
    logging.info(
        f"starting partialator with command line: {partialator_command_line_args}"
    )
    try:
        if (
            args.hkl_file.is_file()
            and args.hkl_file.with_suffix(".hkl1").is_file()
            and args.hkl_file.with_suffix(".hkl2").is_file()
        ):
            logger.info("All hkl files already present, not restarting partialator")
        else:
            partialator = subprocess.run(partialator_command_line_args)

            if partialator.returncode != 0:
                exit_with_error(
                    args,
                    f"error running partialator, error code is {partialator.returncode}",
                )
    except:
        exit_with_error(args, "error running partialator")


if __name__ == "__main__":
    if predefined_args is None:
        exit_with_error(None, "No predefined_args given")
    generate_output(parse_predefined(predefined_args))
