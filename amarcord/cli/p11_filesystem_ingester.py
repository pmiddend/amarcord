import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

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


def validate_crystal_data_reductions(
    crystals: List[P11Crystal],
) -> Tuple[Dict[Path, XDSFilesystem], bool]:
    processed_results: Dict[Path, XDSFilesystem] = {}
    reduction_warnings = False
    for crystal in crystals:
        for r in crystal.runs:
            if r.processed_path is not None:
                filesystem_result = analyze_xds_filesystem(
                    r.processed_path, UnitRegistry()
                )
                if isinstance(filesystem_result, XDSFilesystemError):
                    reduction_warnings = True
                    logger.warning("invalid XDS ingest: %s", filesystem_result.message)
                    if filesystem_result.log_file is not None:
                        logger.warning("log file %s", filesystem_result.log_file)
                else:
                    processed_results[r.processed_path] = filesystem_result
    return processed_results, reduction_warnings


def process_and_validate_with_spreadsheet(
    spreadsheet_lines: List[CrystalLine], crystals: List[P11Crystal]
) -> Tuple[List[P11Crystal], bool]:
    new_crystals: List[P11Crystal] = []
    spreadsheet_warnings = False
    remaining_crystals = set((c.name, c.run_id) for c in spreadsheet_lines)
    for idx, line in enumerate(spreadsheet_lines):
        duplicate_line = find_by(
            spreadsheet_lines[idx + 1 :],
            # pylint: disable=cell-var-from-loop
            lambda csl: csl.name == line.name and csl.run_id == line.run_id,
        )

        if duplicate_line is not None:
            logger.warning(
                "line %s is a duplicate (same name and run id as another row, typo?)",
                idx + 1,
            )
            spreadsheet_warnings = True
    for crystal in crystals:
        new_runs: List[P11Run] = []
        crystal_name: Optional[str] = None
        for run in crystal.runs:
            crystal_spreadsheet_line = find_by(
                spreadsheet_lines,
                # pylint: disable=cell-var-from-loop
                lambda csl: csl.directory == crystal.crystal_id
                and csl.run_id == run.run_id,
            )
            if crystal_spreadsheet_line is None:
                logger.warning(
                    'The crystal and run on the filesystem path "%s/%s" couldn\'t be associated to a line from '
                    "the spreadsheet",
                    crystal.crystal_id,
                    run.run_id,
                )
                spreadsheet_warnings = True
            else:
                if (
                    crystal_name is not None
                    and crystal_name != crystal_spreadsheet_line.name
                ):
                    logger.warning(
                        "different name for same crystal %s given for two different runs",
                        crystal.crystal_id,
                    )
                    spreadsheet_warnings = True
                    new_runs.clear()
                    crystal_name = None
                    break
                crystal_name = crystal_spreadsheet_line.name
                new_runs.append(run)
                remaining_crystals.remove(
                    (crystal_spreadsheet_line.name, crystal_spreadsheet_line.run_id)
                )
        if new_runs and crystal_name is not None:
            new_crystals.append(
                replace(crystal, crystal_id=crystal_name, runs=new_runs)
            )

    if remaining_crystals:
        logger.warning(
            "The following crystals/runs are in the spreadsheet, but not in the filesystem: %s",
            remaining_crystals,
        )
        spreadsheet_warnings = True

    return new_crystals, spreadsheet_warnings


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

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
        dbcontext.metadata, schema=args.analysis_schema
    )

    if args.db_connection_url.startswith("sqlite"):
        logger.info("Creating tables")
        dbcontext.create_all(CreationMode.CHECK_FIRST)

    logger.info("Analyzing filesystem...")
    crystals, warnings = parse_p11_crystals(Path(args.p11_proposal_path))

    if warnings and not args.ignore_diffraction_warnings:
        logger.warning(
            "There were warnings! Please check those carefully and then call with --ignore-diffraction-warnings to "
            "apply the results"
        )
        return 1
    logger.info(
        "Analyzing filesystem...done! %s crystal(s) found%s",
        len(crystals),
        ". Ignoring warnings!" if warnings else "",
    )

    logger.info("Analyzing reductions...")
    processed_results, reduction_warnings = validate_crystal_data_reductions(crystals)

    if reduction_warnings and not args.ignore_reduction_warnings:
        logger.warning(
            "There were reduction warnings! Please check those carefully and then call with "
            "--ignore-reduction-warnings to apply the "
            "results "
        )
        return 2
    logger.info(
        "Analyzing reductions...done! %s reduction(s) found%s",
        len(processed_results.keys()),
        ". Ignoring warnings!" if reduction_warnings else "",
    )

    spreadsheet_lines: List[CrystalLine] = []
    if args.crystal_spreadsheet is not None:
        logger.info("Analyzing spreadsheet...")
        spreadsheet_lines = read_crystal_spreadsheet(Path(args.crystal_spreadsheet))
        new_crystals, spreadsheet_warnings = process_and_validate_with_spreadsheet(
            spreadsheet_lines, crystals
        )

        if spreadsheet_warnings and not args.ignore_spreadsheet_warnings:
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
                if args.create_crystals:
                    logger.info("Creating the crystals (without puck information)...")
                    for crystal in crystals:
                        if (
                            conn.execute(
                                sa.select([table_crystals_.c.crystal_id]).where(
                                    table_crystals_.c.crystal_id == crystal.crystal_id
                                )
                            ).fetchone()
                            is None
                        ):
                            logger.info("Creating crystal %s...", crystal.crystal_id)
                            conn.execute(
                                sa.insert(table_crystals_).values(
                                    crystal_id=crystal.crystal_id
                                )
                            )
                    logger.info("Creating the crystals...Done!")

                logger.info("Ingesting diffractions...")
                has_warnings = ingest_diffractions_for_crystals(
                    conn,
                    table_diffs_,
                    table_crystals_,
                    crystals,
                    args.crystal_spreadsheet is not None,
                    metadata_retriever=metadata_retriever_from_lines(
                        spreadsheet_lines, args.detector_name
                    )
                    if spreadsheet_lines
                    else empty_metadata_retriever(args.detector_name),
                )
                if has_warnings and not args.ignore_db_warnings:
                    logger.warning(
                        "There were ingestion warnings! Please check those carefully and then call with "
                        "--ignore-db-warnings to apply the results"
                    )
                    return 3
                logger.info(
                    "Ingesting diffractions...done!%s",
                    ". Ignoring warnings!" if has_warnings else "",
                )
                logger.info("Ingesting reductions...")
                has_reduction_warnings = ingest_reductions_for_crystals(
                    conn, table_data_reduction_, crystals, processed_results
                )
                if has_reduction_warnings and not args.ignore_db_warnings:
                    logger.warning(
                        "There were reduction warnings! Please check those carefully and then call with "
                        "--ignore-db-warnings to apply the results"
                    )
                    return 4
                logger.info(
                    "Ingesting reductions...Done!%s",
                    ". Ignoring warnings!" if has_reduction_warnings else "",
                )
                if args.dry_run or (
                    has_reduction_warnings
                    or has_warnings
                    and not args.ignore_db_warnings
                ):
                    raise StopIteration()
                logger.info("Ingest complete!")
        except StopIteration:
            logger.info("Ingested the results as a dry-run!")

    return 0 if not warnings else 1


if __name__ == "__main__":
    sys.exit(main())
