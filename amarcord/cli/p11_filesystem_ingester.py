import logging
import sys
from dataclasses import replace
from math import ceil
from pathlib import Path
from time import sleep
from time import time
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import sqlalchemy as sa
from pint import UnitRegistry
from tap import Tap

from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.analyze_filesystem import parse_p11_crystals
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db_ingest import EIGER_16_M_DETECTOR_NAME
from amarcord.amici.p11.db_ingest import empty_metadata_retriever
from amarcord.amici.p11.db_ingest import ingest_diffractions_for_crystals
from amarcord.amici.p11.db_ingest import ingest_reductions_for_crystals
from amarcord.amici.p11.spreadsheet_reader import CrystalLine
from amarcord.amici.p11.spreadsheet_reader import metadata_retriever_from_lines
from amarcord.amici.p11.spreadsheet_reader import read_crystal_spreadsheet
from amarcord.amici.xds.analyze_filesystem import XDSFilesystem
from amarcord.amici.xds.analyze_filesystem import XDSFilesystemError
from amarcord.amici.xds.analyze_filesystem import analyze_xds_filesystem
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.util import find_by

DUMMY_PUCK_ID = "P1"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # output SQL statements?
    force_diffraction_ingest: bool = (
        False  # force creation of (successful) diffractions if missing
    )
    p11_proposal_path: str  # Path to the proposal root
    detector_name: str = EIGER_16_M_DETECTOR_NAME  # Detector name to use in the diffractions (if non-existant)
    crystal_spreadsheet: Optional[
        str
    ] = None  # Path to a spreadsheet file containing crystal information
    ignore_diffraction_warnings: bool = (
        False  # Ignore warnings related to ingestion of diffractions
    )
    ignore_reduction_warnings: bool = (
        False  # Ignore warnings related to ingestion of reductions
    )
    ignore_spreadsheet_warnings: bool = (
        False  # Ignore warnings related spreadsheet results
    )
    ignore_db_warnings: bool = False  # Ignore warnings related to ingestion to the DB
    dry_run: bool = False  # Dry run, don't commit the results
    create_crystals: bool = (
        False  # Create crystals if they don't exist (just for debugging purposes)
    )
    main_schema: Optional[str] = None
    analysis_schema: Optional[str] = None

    """Ingest P11 filesystem"""


def parse_reductions_for_crystals(
    crystals: List[P11Crystal],
) -> Tuple[Dict[Path, XDSFilesystem], List[str]]:
    processed_results: Dict[Path, XDSFilesystem] = {}
    reduction_warnings: List[str] = []
    for crystal in crystals:
        for r in crystal.runs:
            if r.processed_path is not None:
                filesystem_result = analyze_xds_filesystem(
                    r.processed_path, UnitRegistry()
                )
                if isinstance(filesystem_result, XDSFilesystemError):
                    reduction_warnings.append(
                        f"invalid XDS ingest: {filesystem_result.message}"
                    )
                    if filesystem_result.log_file is not None:
                        reduction_warnings.append(
                            f"log file {filesystem_result.log_file}"
                        )
                else:
                    processed_results[r.processed_path] = filesystem_result
    return processed_results, reduction_warnings


def process_and_validate_with_spreadsheet(
    spreadsheet_lines: List[CrystalLine], crystals: List[P11Crystal]
) -> Tuple[List[P11Crystal], List[str]]:
    new_crystals: List[P11Crystal] = []
    spreadsheet_warnings: List[str] = []
    # First step: check for duplicate lines in the spreadsheet file
    for idx, line in enumerate(spreadsheet_lines):
        duplicate_line: Optional[Tuple[int, CrystalLine]] = find_by(
            list(enumerate(spreadsheet_lines[idx + 1 :])),
            # pylint: disable=cell-var-from-loop
            lambda csl: csl[1].name == line.name and csl[1].run_id == line.run_id,
        )

        if duplicate_line is not None:
            spreadsheet_warnings.append(
                f"line {idx+1} is a duplicate (same name {line.name} and run id {line.run_id} as row {duplicate_line[0]}, typo?)"
            )
    # Second step: Iterate over all known crystals from the filesystem, keep
    # track of the ones we have _not_ seen in the spreadsheet
    remaining_crystals = set((c.name, c.run_id) for c in spreadsheet_lines)
    for crystal in crystals:
        new_runs: List[P11Run] = []
        crystal_name: Optional[str] = None
        for run in crystal.runs:
            # Try to find the crystal in the spreadsheet
            crystal_spreadsheet_line = find_by(
                spreadsheet_lines,
                # pylint: disable=cell-var-from-loop
                lambda csl: csl.directory == crystal.crystal_id
                and csl.run_id == run.run_id,
            )
            if crystal_spreadsheet_line is None:
                spreadsheet_warnings.append(
                    f'The crystal and run on the filesystem path "{crystal.crystal_id}/{run.run_id}"couldn\'t be '
                    f"associated to a line from the spreadsheet"
                )
            else:
                if (
                    crystal_name is not None
                    and crystal_name != crystal_spreadsheet_line.name
                ):
                    spreadsheet_warnings.append(
                        f"different name for same crystal {crystal.crystal_id} given for two different runs"
                    )
                    new_runs.clear()
                    crystal_name = None
                    break
                crystal_name = crystal_spreadsheet_line.name
                new_runs.append(run)
                crystal_tuple = (
                    crystal_spreadsheet_line.name,
                    crystal_spreadsheet_line.run_id,
                )
                # Due to duplicate lines, we might not have this tuple in the collection anymore
                if crystal_tuple in remaining_crystals:
                    remaining_crystals.remove(
                        (crystal_spreadsheet_line.name, crystal_spreadsheet_line.run_id)
                    )
        if new_runs and crystal_name is not None:
            new_crystals.append(
                replace(crystal, crystal_id=crystal_name, runs=new_runs)
            )

    if remaining_crystals:
        spreadsheet_warnings.append(
            f"The following crystals/runs are in the spreadsheet, but not in the filesystem: {remaining_crystals}"
        )

    return new_crystals, spreadsheet_warnings


def _crystal_exists(
    conn: Connection, table_crystals_: sa.Table, crystal_id: str
) -> bool:
    return (
        conn.execute(
            sa.select([table_crystals_.c.crystal_id]).where(
                table_crystals_.c.crystal_id == crystal_id
            )
        ).fetchone()
        is not None
    )


def main_loop(
    args: Arguments,
    dbcontext: DBContext,
    table_crystals_: sa.Table,
    table_diffs_: sa.Table,
    table_data_reduction_: sa.Table,
) -> Union[int, Tuple[List[P11Crystal], Dict[Path, XDSFilesystem]]]:
    logger.info("Analyzing filesystem...")
    crystals, warnings = parse_p11_crystals(Path(args.p11_proposal_path))

    if warnings:
        for warning in warnings:
            logger.warning(warning)
        if args.ignore_diffraction_warnings:
            logger.info("Skipping %s diffraction warning(s)", len(warnings))
        else:
            logger.warning(
                "There were warnings! Please check those carefully and then call with --ignore-diffraction-warnings to "
                "apply the results"
            )
            return 1
    logger.info(
        "Analyzing filesystem...done! %s crystal(s) found",
        len(crystals),
    )

    logger.info("Analyzing reductions...")
    reduction_results, reduction_warnings = parse_reductions_for_crystals(crystals)

    if reduction_warnings:
        for warning in reduction_warnings:
            logger.warning(warning)
        if args.ignore_reduction_warnings:
            logger.info("Skipping %s reduction warning(s)", len(reduction_warnings))
        else:
            logger.warning(
                "There were reduction warnings! Please check those carefully and then call with "
                "--ignore-reduction-warnings to apply the "
                "results "
            )
            return 2
    logger.info(
        "Analyzing reductions...done! %s reduction(s) found",
        len(reduction_results.keys()),
    )

    spreadsheet_lines: List[CrystalLine] = []
    if args.crystal_spreadsheet is not None:
        logger.info("Analyzing spreadsheet...")
        spreadsheet_lines = read_crystal_spreadsheet(Path(args.crystal_spreadsheet))
        new_crystals, spreadsheet_warnings = process_and_validate_with_spreadsheet(
            spreadsheet_lines, crystals
        )

        if spreadsheet_warnings:
            for warning in spreadsheet_warnings:
                logger.warning(warning)
            if args.ignore_spreadsheet_warnings:
                logger.info(
                    "skipping %s spreadsheet warning(s)", len(spreadsheet_warnings)
                )
            else:
                logger.warning(
                    "There were spreadsheet warnings! Please check those carefully and then call with "
                    "--ignore-spreadsheet-warnings to apply the "
                    "results "
                )
                return 5

        crystals = new_crystals
        logger.info("Analyzing spreadsheet...Done!")

    with dbcontext.connect() as conn:
        try:
            with conn.begin():
                logger.info("Ingesting into DB...")
                to_remove_crystal_ids: Set[str] = set()
                for crystal in crystals:
                    if not _crystal_exists(conn, table_crystals_, crystal.crystal_id):
                        if args.create_crystals:
                            logger.info("Creating crystal %s...", crystal.crystal_id)
                            conn.execute(
                                sa.insert(table_crystals_).values(
                                    crystal_id=crystal.crystal_id
                                )
                            )
                        else:
                            to_remove_crystal_ids.add(crystal.crystal_id)

                if to_remove_crystal_ids:
                    logger.info(
                        "Removing %s crystal(s) which are not in the DB...",
                        len(to_remove_crystal_ids),
                    )
                    crystals = [
                        c for c in crystals if c.crystal_id not in to_remove_crystal_ids
                    ]

                logger.info("Ingesting diffractions...")
                diff_ingest_warnings = ingest_diffractions_for_crystals(
                    conn,
                    table_diffs_,
                    table_crystals_,
                    crystals,
                    args.crystal_spreadsheet is not None
                    or args.force_diffraction_ingest,
                    metadata_retriever=metadata_retriever_from_lines(
                        spreadsheet_lines, args.detector_name
                    )
                    if spreadsheet_lines
                    else empty_metadata_retriever(args.detector_name),
                )
                if diff_ingest_warnings:
                    for warning in diff_ingest_warnings:
                        logger.warning(warning)
                    if args.ignore_db_warnings:
                        logger.info(
                            "skipping %s diffraction ingest warnings",
                            len(diff_ingest_warnings),
                        )
                    else:
                        logger.warning(
                            "There were ingestion warnings! Please check those carefully and then call with "
                            "--ignore-db-warnings to apply the results"
                        )
                        return 3
                logger.info(
                    "Ingesting diffractions...done!",
                )
                logger.info("Ingesting reductions...")
                reduction_warnings = ingest_reductions_for_crystals(
                    conn,
                    table_data_reduction_,
                    table_diffs_,
                    crystals,
                    reduction_results,
                )
                if reduction_warnings:
                    for warning in reduction_warnings:
                        logger.warning(warning)
                    if args.ignore_db_warnings:
                        logger.info(
                            "Skipping %s reduction ingest warning(s)",
                            len(reduction_warnings),
                        )
                    else:
                        logger.warning(
                            "There were reduction warnings! Please check those carefully and then call with "
                            "--ignore-db-warnings to apply the results"
                        )
                    return 4
                logger.info("Ingesting reductions...Done!")
                if args.dry_run or (
                    reduction_warnings
                    or diff_ingest_warnings
                    and not args.ignore_db_warnings
                ):
                    raise StopIteration()
                logger.info("Ingest complete!")
        except StopIteration:
            logger.info("Ingested the results as a dry-run!")
    return crystals, reduction_results


def main(args: Arguments) -> int:
    dbcontext = DBContext(args.db_connection_url, echo=args.db_echo)

    table_crystals_ = table_crystals(
        dbcontext.metadata,
        table_pucks(dbcontext.metadata, schema=args.main_schema),
        schema=args.main_schema,
    )
    table_diffs_ = table_diffractions(
        dbcontext.metadata, table_crystals_, schema=args.main_schema
    )
    table_data_reduction_ = table_data_reduction(
        dbcontext.metadata, table_crystals_, schema=args.analysis_schema
    )

    if args.db_connection_url.startswith("sqlite"):
        logger.info("Creating tables")
        dbcontext.create_all(CreationMode.CHECK_FIRST)

    while True:
        logger.info("=====================================")
        logger.info("==========STARTING INGEST============")
        logger.info("=====================================")
        before = time()
        result = main_loop(
            args,
            dbcontext,
            table_crystals_,
            table_diffs_,
            table_data_reduction_,
        )
        after = time()
        if isinstance(result, int):
            return result
        logger.info("=====================================")
        logger.info("==========FINISHED INGEST============")
        logger.info("=====================================")
        if after - before < 5:
            logger.info("Next iteration in %ss", ceil(5 - (after - before)))
            sleep(ceil(5 - (after - before)))


if __name__ == "__main__":
    sys.exit(main(Arguments(underscores_to_dashes=True).parse_args()))
