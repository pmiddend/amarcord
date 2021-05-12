import logging
import sys
from pathlib import Path
from typing import Dict
from typing import Optional

import sqlalchemy as sa
from pint import UnitRegistry
from tap import Tap

from amarcord.amici.p11.analyze_filesystem import parse_p11_crystals
from amarcord.amici.p11.db import PuckType
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db_ingest import ingest_diffractions_for_crystals
from amarcord.amici.p11.db_ingest import ingest_reductions_for_crystals
from amarcord.amici.xds.analyze_filesystem import XDSFilesystem
from amarcord.amici.xds.analyze_filesystem import XDSFilesystemError
from amarcord.amici.xds.analyze_filesystem import analyze_xds_filesystem
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

DUMMY_PUCK_ID = "P1"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # output SQL statements?
    p11_proposal_path: str  # Path to the proposal root
    ignore_diffraction_warnings: bool = (
        False  # Ignore warnings related to ingestion of diffractions
    )
    ignore_reduction_warnings: bool = (
        False  # Ignore warnings related to ingestion of reductions
    )
    ignore_db_warnings: bool = False  # Ignore warnings related to ingestion to the DB
    dry_run: bool = False  # Dry run, don't commit the results
    create_crystals: bool = (
        False  # Create crystals if they don't exist (just for debugging purposes)
    )
    main_schema: Optional[str] = None
    analysis_schema: Optional[str] = None

    """Ingest P11 filesystem"""


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.db_connection_url, echo=args.db_echo)

    table_pucks_ = table_pucks(dbcontext.metadata, schema=args.main_schema)
    table_crystals_ = table_crystals(dbcontext.metadata, schema=args.main_schema)
    table_diffs_ = table_diffractions(dbcontext.metadata, schema=args.main_schema)
    table_dewar_lut(dbcontext.metadata, schema=args.main_schema)
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
        "Analysis complete %s crystal(s), analyzing data reduction...", len(crystals)
    )
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

    if reduction_warnings and not args.ignore_reduction_warnings:
        logger.warning(
            "There were reduction warnings! Please check those carefully and then call with "
            "--ignore-reduction-warnings to apply the "
            "results "
        )
        return 2

    with dbcontext.connect() as conn:
        try:
            with conn.begin():
                if args.create_crystals:
                    conn.execute(
                        sa.insert(table_pucks_).values(
                            puck_id=DUMMY_PUCK_ID, puck_type=PuckType.UNI, owner="dummy"
                        )
                    )

                has_warnings = ingest_diffractions_for_crystals(
                    conn,
                    table_diffs_,
                    table_crystals_,
                    crystals,
                    dummy_puck=DUMMY_PUCK_ID if args.create_crystals else None,
                )
                if has_warnings and not args.ignore_db_warnings:
                    logger.warning(
                        "There were ingestion warnings! Please check those carefully and then call with "
                        "--ignore-db-warnings to apply the results"
                    )
                    return 3
                has_reduction_warnings = ingest_reductions_for_crystals(
                    conn,
                    table_data_reduction_,
                    crystals,
                    UnitRegistry(),
                )
                if has_reduction_warnings and not args.ignore_db_warnings:
                    logger.warning(
                        "There were reduction warnings! Please check those carefully and then call with "
                        "--ignore-db-warnings to apply the results"
                    )
                    return 4
                if args.dry_run or (
                    has_reduction_warnings
                    or has_warnings
                    and not args.ignore_db_warnings
                ):
                    raise StopIteration()
                logger.info("Ingested the results!")
        except StopIteration:
            logger.info("Ingested the results as a dry-run")

    return 0 if not warnings else 1


if __name__ == "__main__":
    sys.exit(main())
