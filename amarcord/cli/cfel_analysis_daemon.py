import asyncio
import glob
import logging
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from dataclasses import replace
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from time import sleep
from typing import Dict, Iterable
from typing import List
from typing import Optional
from typing import cast

import tqdm
from tap import Tap

from amarcord.db.analysis_result import DBCFELAnalysisResult
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap, run_matches_dataset
from amarcord.db.attributo_type import AttributoType, AttributoTypeSample
from amarcord.db.data_set import DBDataSet
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import CreationMode, Connection
from amarcord.db.table_classes import DBRun
from amarcord.db.tables import create_tables_from_metadata
from amarcord.util import last_existing_dir, replace_illegal_path_characters

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    base_directory: Path  # Base directory for raw-processed
    list_of_blocks_raw_directory: Optional[
        Path
    ] = None  # Directory containing the runs and h5 files for the runs
    list_of_blocks_output_dir: Optional[
        Path
    ] = None  # Directory where to put the list of blocks
    list_of_blocks_pattern: Optional[
        str
    ] = None  # Pattern to filter files for list of file generation
    list_of_blocks_extension: Optional[
        str
    ] = None  # Pattern to filter extension for list of file generation
    wait_time_seconds: float = 10.0  # How much to wait between analysis runs
    debug: bool = False  # Enable debug mode (creating the database, adding runs instead of assuming they exist)
    verbose: bool = False  # Show more log messages
    parallelization: int = 5  # How many processes to spawn
    filename_infix: Optional[str] = None  # Optional infix to stream and err files
    process_after_run_id: Optional[
        int
    ] = None  # Only process run blocks with at least this run ID (as "from")
    dry_run: bool = False  # Don't issue any SQL commits


@dataclass(frozen=True)
class RunBlock:
    dir_name: str
    run_from: int
    run_to: int


def run_block_in_parallel(
    bd: Path, infix: Optional[str], run_block: RunBlock
) -> Optional[DBCFELAnalysisResult]:
    try:
        logger.debug("block %s: start processing", run_block)

        result = read_analysis_for_run_block(bd, infix, run_block)

        logger.info("block %s: complete", run_block.dir_name)

        logger.debug("block %s: processing success", run_block)

        return result
    except RunBlockIncomplete as e:
        logger.debug("incomplete block: %s", e)
        logger.info("block %s: incomplete", run_block.dir_name)
        return None
    except:
        logger.exception("block %s: error", run_block.dir_name)
        return None


def parse_pathname(dir_name: str) -> Optional[RunBlock]:
    result_range = re.match(r"^(\d+)-(\d+)-.*$", dir_name)
    if result_range is not None:
        return RunBlock(
            dir_name, int(result_range.group(1)), int(result_range.group(2))
        )
    result_single = re.match(r"^(\d+)-.*$", dir_name)
    if result_single is not None:
        run_id = int(result_single.group(1))
        return RunBlock(dir_name, run_from=run_id, run_to=run_id)
    return None


def outer_shell(CCstar_dat_file):
    shell, CCstar_shell = "", ""
    with open(CCstar_dat_file, "r", encoding="utf-8") as file:
        for line in file:
            line = re.sub(" +", " ", line.strip()).split(" ")
            CCstar_shell = line[1]
            shell = line[3]
    return shell, CCstar_shell


indexes = [
    "Resolution",
    "Rsplit (%)",
    "CC1/2",
    "CC*",
    "SNR",
    "Completeness (%)",
    "Multiplicity",
    "Total Measurements",
    "Unique Reflections",
    "Wilson B-factor",
    "Outer shell info",
]


@dataclass(frozen=True)
class IndexingResults:
    num_patterns: int
    num_hits: int
    indexed_patterns: int
    indexed_crystals: int


def parse_stream_file_for_indexing_results(fp: Path) -> Optional[IndexingResults]:
    try:
        res_hits = (
            subprocess.check_output(["grep", "-rc", "hit = 1", str(fp)])
            .decode("utf-8")
            .strip()
            .split("\n", maxsplit=1)
        )
        hits = int(res_hits[0])
        chunks = int(
            subprocess.check_output(["grep", "-c", "Image filename", str(fp)])
            .decode("utf-8")
            .strip()
            .split("\n", maxsplit=1)[0]
        )
    except subprocess.CalledProcessError:
        hits = 0
        chunks = 0

    try:
        res_indexed = (
            subprocess.check_output(["grep", "-rc", "Begin crystal", str(fp)])
            .decode("utf-8")
            .strip()
            .split("\n", maxsplit=1)
        )
        indexed = int(res_indexed[0])
    except subprocess.CalledProcessError:
        indexed = 0

    try:
        res_none_indexed_patterns = (
            subprocess.check_output(["grep", "-rc", "indexed_by = none", str(fp)])
            .decode("utf-8")
            .strip()
            .split("\n", maxsplit=1)
        )
        none_indexed_patterns = int(res_none_indexed_patterns[0])
    except subprocess.CalledProcessError:
        none_indexed_patterns = 0

    indexed_patterns = chunks - none_indexed_patterns

    return IndexingResults(
        num_patterns=chunks,
        num_hits=hits,
        indexed_patterns=indexed_patterns,
        indexed_crystals=indexed,
    )


def parse_err(filename, data_info):
    resolution = None
    Rsplit = None
    CC = None
    CCstar = None
    snr = None
    completeness = None
    multiplicity = None  # it is the same as redundancy
    total_measurements = None
    unique_reflections = None
    Wilson_B_factor = None
    with open(filename, "r", encoding="utf-8") as file:
        for line in file:

            if line.startswith("Overall CC* = "):
                CCstar = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("Overall Rsplit = "):
                Rsplit = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("Overall CC = "):
                CC = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("Fixed resolution range: "):
                resolution = (
                    line[line.find("(") + 1 : line.find(")")]
                    .replace("to", "-")
                    .replace("Angstroms", "")
                    .strip()
                )
            if " measurements in total." in line:
                total_measurements = int(re.search(r"\d+", line).group(0))
            if " reflections in total." in line:
                unique_reflections = int(re.search(r"\d+", line).group(0))
            if line.startswith("Overall <snr> ="):
                snr = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("Overall redundancy ="):
                multiplicity = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("Overall completeness ="):
                completeness = float(re.search(r"\d+\.\d+", line).group(0))
            if line.startswith("B ="):
                Wilson_B_factor = float(re.search(r"\d+\.\d+", line).group(0))

    if all(
        [
            resolution,
            Rsplit,
            CC,
            CCstar,
            snr,
            completeness,
            multiplicity,
            total_measurements,
            unique_reflections,
            Wilson_B_factor,
        ]
    ):
        data_info["Resolution"] = resolution
        data_info["Rsplit (%)"] = Rsplit
        data_info["CC1/2"] = CC
        data_info["CC*"] = CCstar
        data_info["SNR"] = snr
        data_info["Completeness (%)"] = completeness
        data_info["Multiplicity"] = multiplicity
        data_info["Total Measurements"] = total_measurements
        data_info["Unique Reflections"] = unique_reflections
        data_info["Wilson B-factor"] = Wilson_B_factor


def processing(block_name, main_path, list_of_files, CCstar_dat_files):
    data_info = {i: "" for i in indexes}
    if len(CCstar_dat_files) > 0:
        CCstar_dat_file = max(CCstar_dat_files, key=os.path.getctime)

        name_of_run = os.path.split(CCstar_dat_file)
        name_of_run = (
            name_of_run[0]
            .replace(main_path, "")
            .replace("/j_stream", "")
            .replace("/", "")
        )

        shell, CCstar_shell = outer_shell(CCstar_dat_file)

        try:
            data_info[
                "Outer shell info"
            ] = f"{float(shell):.3f} - {float(CCstar_shell):.3f}"
        except:
            data_info["Outer shell info"] = f"{shell} - {CCstar_shell}"
        if len(list_of_files) > 0 and len(name_of_run) > 0:
            filename_to_parse = max(list_of_files, key=os.path.getctime)

            logger.debug("%s: processing err file: %s", block_name, filename_to_parse)

            parse_err(filename_to_parse, data_info)
        else:
            logger.debug(
                "%s: no err file found (or name of run is empty: %s)",
                block_name,
                name_of_run,
            )
    else:
        logger.debug("%s: no ccstar dat file(s) found", block_name)

    return data_info


class RunBlockIncomplete(Exception):
    pass


def read_analysis_for_run_block(
    base_directory: Path, filename_infix_: Optional[str], run_block: RunBlock
) -> DBCFELAnalysisResult:
    # add * if an infix is given because it could be amb-xg- instead of just xg-
    filename_infix = "" if filename_infix_ is None else "*" + filename_infix_
    list_of_files = glob.glob(
        f"{base_directory}/*{run_block.dir_name}*/**/{filename_infix}*.err",
        recursive=True,
    )

    CCstar_dat_files = glob.glob(
        f"{base_directory}/*{run_block.dir_name.strip()}*/**/{filename_infix}*CCstar.dat",
        recursive=True,
    )

    stream_files = glob.glob(
        f"{base_directory}/*{run_block.dir_name.strip()}*/j_stream/{filename_infix}*stream",
    )

    indexing_results: IndexingResults
    if stream_files:
        logger.debug(
            "block %s: reading stream file %s", run_block.dir_name, stream_files[0]
        )
        indexing_results_from_file = parse_stream_file_for_indexing_results(
            Path(stream_files[0])
        )
        if indexing_results_from_file is not None:
            indexing_results = indexing_results_from_file
        else:
            indexing_results = IndexingResults(
                num_patterns=0, num_hits=0, indexed_patterns=0, indexed_crystals=0
            )
    else:
        indexing_results = IndexingResults(
            num_patterns=0, num_hits=0, indexed_patterns=0, indexed_crystals=0
        )

    logger.debug("block %s: reading processing results", run_block.dir_name)
    data_info = processing(
        run_block.dir_name, base_directory, list_of_files, CCstar_dat_files
    )

    def get_safe_float(s):
        v = data_info.get(s, None)
        if v is None:
            raise RunBlockIncomplete(f'value "{s}" not found')
        if isinstance(v, str) and not v:
            raise RunBlockIncomplete(
                f"float value {s} is empty, total dict: {data_info}"
            )
        if not isinstance(v, float):
            raise Exception(f'value "{s}" not float but {type(v)}: {v}')
        return v

    def get_safe_int(s):
        v = data_info.get(s, None)
        if v is None:
            raise RunBlockIncomplete(f'value "{s}" not found')
        if isinstance(v, str) and not v:
            raise RunBlockIncomplete(f"int value {v} is empty")
        if not isinstance(v, int):
            raise Exception(f'value "{s}" not int but {type(v)}: {v}')
        return v

    def get_safe_str(s):
        v = data_info.get(s, None)
        if v is None:
            raise RunBlockIncomplete(f'value "{s}" not found')
        if not isinstance(v, str):
            raise Exception(f'value "{s}" not str but {type(v)}: {v}')
        return v

    return DBCFELAnalysisResult(
        directory_name=run_block.dir_name,
        run_from=run_block.run_from,
        run_to=run_block.run_to,
        resolution=get_safe_str("Resolution"),
        rsplit=get_safe_float("Rsplit (%)"),
        cchalf=get_safe_float("CC1/2"),
        ccstar=get_safe_float("CC*"),
        snr=get_safe_float("SNR"),
        completeness=get_safe_float("Completeness (%)"),
        multiplicity=get_safe_float("Multiplicity"),
        total_measurements=get_safe_int("Total Measurements"),
        unique_reflections=get_safe_int("Unique Reflections"),
        wilson_b=get_safe_float("Wilson B-factor"),
        outer_shell=get_safe_str("Outer shell info"),
        num_hits=indexing_results.num_hits,
        num_patterns=indexing_results.num_patterns,
        indexed_patterns=indexing_results.indexed_patterns,
        indexed_crystals=indexing_results.indexed_crystals,
        # Leave comment free for now, we re-add it back later
        comment="",
    )


async def list_of_files_iteration(
    db: AsyncDB,
    pattern: str,
    extension: str,
    input_directory: Path,
    output_directory: Path,
) -> None:
    async with db.connect() as conn:
        await generate_list_of_files(
            db, conn, pattern, extension, input_directory, output_directory
        )


def generate_data_set_file_name(
    ds: DBDataSet, sample_id_to_name: Dict[int, str], attributi: List[DBAttributo]
) -> str:
    sample_attributi = [
        a for a in attributi if isinstance(a.attributo_type, AttributoTypeSample)
    ]
    sample_names: List[str] = []
    for a in sample_attributi:
        sample_id = ds.attributi.select_sample_id(a.name)
        if sample_id is not None:
            sample_name = sample_id_to_name.get(sample_id)
            if sample_name is not None:
                sample_names.append(sample_name)
    return f"ds-{ds.id}-{','.join(replace_illegal_path_characters(s) for s in sample_names)}.lst"


async def generate_list_of_files(
    db: AsyncDB,
    conn: Connection,
    pattern: str,
    extension: str,
    input_directory: Path,
    output_directory: Path,
) -> None:
    if not input_directory.is_dir():
        raise Exception(
            f"couldn't find input directory {input_directory}; last existing dir {last_existing_dir(input_directory)}"
        )
    if not output_directory.is_dir():
        raise Exception(
            f"couldn't find output directory {output_directory}; last existing dir {last_existing_dir(output_directory)}"
        )

    attributi = await db.retrieve_attributi(conn, associated_table=None)
    samples = await db.retrieve_samples(conn, attributi)
    data_sets = await db.retrieve_data_sets(
        conn,
        [s.id for s in samples],
        attributi,
    )
    runs = await db.retrieve_runs(conn, attributi)

    def find_run_files(this_run: DBRun) -> Iterable[Path]:
        run_dir = input_directory / str(this_run.id)
        if not run_dir.is_dir():
            return ()
        return (
            x for x in run_dir.iterdir() if extension in x.name and pattern in x.name
        )

    def write_run_files(file_name: str, files: Iterable[Path]) -> None:
        with (output_directory / file_name).open("w") as output_file:
            for f in files:
                output_file.write(f"{f}\n")

    for ds in data_sets:
        run_files: List[Path] = []
        for r in runs:
            if not run_matches_dataset(r.attributi, ds.attributi):
                continue
            run_files.extend(find_run_files(r))
        if run_files:
            logger.info(
                f"ds {ds.id}: found {len(run_files)} matching run files, generating file {output_directory}/ds-{ds.id}.lst"
            )
            write_run_files(
                generate_data_set_file_name(
                    ds,
                    sample_id_to_name={s.id: s.name for s in samples},
                    attributi=attributi,
                ),
                run_files,
            )
        else:
            logger.info(f"ds {ds.id}: found no matching runs, skipping")


def mymain(args: Arguments) -> int:
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    dbcontext = AsyncDBContext(args.db_connection_url, echo=False)
    db = AsyncDB(dbcontext, create_tables_from_metadata(dbcontext.metadata))

    if args.debug:
        dbcontext.create_all(CreationMode.CHECK_FIRST)

    base_directory = Path(args.base_directory)
    if not base_directory.is_dir():
        logger.error(
            "Directory %s doesn't exist, cannot start like this", base_directory
        )
        return 1

    while True:
        try:
            if (
                args.list_of_blocks_pattern is not None
                and args.list_of_blocks_output_dir is not None
                and args.list_of_blocks_raw_directory is not None
                and args.list_of_blocks_extension is not None
            ):
                asyncio.run(
                    list_of_files_iteration(
                        db,
                        args.list_of_blocks_pattern,
                        args.list_of_blocks_extension,
                        args.list_of_blocks_raw_directory,
                        args.list_of_blocks_output_dir,
                    )
                )

            # FIXME for now
            # asyncio.run(main_loop_iteration(args, base_directory, db))
        except:
            logger.exception("Loop iteration exception, waiting and then continuing...")

        logger.info("waiting for next check")
        sleep(args.wait_time_seconds)


async def main_loop_iteration(
    args: Arguments, base_directory: Path, db: AsyncDB
) -> None:
    run_blocks: List[RunBlock] = cast(
        List[RunBlock],
        [
            parse_pathname(x.name)
            for x in base_directory.iterdir()
            if parse_pathname(x.name) is not None
        ],
    )
    if args.process_after_run_id is not None:
        run_blocks = [x for x in run_blocks if x.run_from > args.process_after_run_id]
    # We map each run ID to the block it's in (and overwrite if we've got overlap between
    # the blocks - shouldn't happen though)
    run_to_dir_name: Dict[int, str] = {}
    for run_block in run_blocks:
        for run_id in range(run_block.run_from, run_block.run_to + 1):
            run_to_dir_name[run_id] = run_block.dir_name
    # Do the analysis on each block in a separate process, speeding up the whole thing substantially
    with Pool(args.parallelization) as p:
        logger.info("start analyzing run blocks...")
        analysis_results_opt = tqdm.tqdm(
            p.imap_unordered(
                partial(
                    run_block_in_parallel, str(base_directory), args.filename_infix
                ),
                run_blocks,
            ),
            total=len(run_blocks),
        )

        analysis_results = [x for x in analysis_results_opt if x is not None]
    with db.connect() as conn:
        with conn.begin():
            logger.info("run blocks analyzed, updating data base")
            # we use this to compare what runs are in blocks and which ones are in the DB
            # if there's a run in a block that the DB doesn't know about, we might decide to just create it
            run_ids = set(await db.retrieve_run_ids(conn))
            block_to_comment: Dict[str, str] = {
                r.directory_name: r.comment
                for r in await db.retrieve_cfel_analysis_results(conn)
            }

            for run_id, dir_name in run_to_dir_name.items():
                if run_id not in run_ids and args.debug:
                    if args.debug:
                        logger.info("adding run %s", run_id)
                        await db.create_run(
                            conn,
                            run_id,
                            attributi=AttributiMap.from_types_and_json(
                                await db.retrieve_attributi(conn, AssociatedTable.RUN),
                                await db.retrieve_sample_ids(conn),
                                {},
                            ),
                        )
                    else:
                        logger.error(
                            "couldn't find run ID %s in table, but is in directory %s",
                            run_id,
                            dir_name,
                        )

            logger.info("clearing analysis results")
            await db.clear_cfel_analysis_results(conn, args.process_after_run_id)
            for analysis_result in analysis_results:
                logger.info("adding %s", analysis_result)
                await db.create_cfel_analysis_result(
                    conn,
                    replace(
                        analysis_result,
                        comment=block_to_comment.get(
                            analysis_result.directory_name, ""
                        ),
                    ),
                )
            logger.info("updated data base")

            if args.dry_run:
                logger.info("dry run, excepting")
                raise Exception("dry run, don't commit")


if __name__ == "__main__":
    sys.exit(mymain(Arguments(underscores_to_dashes=True).parse_args()))
