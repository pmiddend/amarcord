import logging
import sys
from pathlib import Path
from typing import Dict

from pint import UnitRegistry
from tap import Tap

from amarcord.amici.p11.analyze_filesystem import parse_p11_targets
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db_ingest import ingest_diffractions_for_targets
from amarcord.amici.p11.db_ingest import ingest_reductions_for_targets
from amarcord.amici.xds.analyze_filesystem import XDSFilesystem
from amarcord.amici.xds.analyze_filesystem import XDSFilesystemError
from amarcord.amici.xds.analyze_filesystem import analyze_xds_filesystem
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # output SQL statements?
    p11_proposal_path: str  # Path to the proposal root
    force: bool = False  # Force ingest, despite warnings
    dry_run: bool = False  # Dry run, don't commit the results
    main_schema: str = "SARS_COV_2_test_v2"
    analysis_schema: str = "SARS_COV_2_Analysis_test_v2"

    """Ingest P11 filesystem"""


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.db_connection_url, echo=args.db_echo)

    logger.info("Analyzing filesystem...")
    targets, warnings = parse_p11_targets(Path(args.p11_proposal_path))

    if warnings and not args.force:
        logger.warning(
            "There were warnings! Please check those carefully and then call with --force to apply the results"
        )
        return 1

    logger.info("Analysis complete, analyzing data reduction...")
    processed_results: Dict[Path, XDSFilesystem] = {}
    reduction_warnings = False
    for t in targets:
        for puck in t.pucks:
            for r in puck.runs:
                if r.processed_path is not None:
                    filesystem_result = analyze_xds_filesystem(
                        r.processed_path, UnitRegistry()
                    )
                    if isinstance(filesystem_result, XDSFilesystemError):
                        reduction_warnings = True
                        logger.warning(
                            "invalid XDS ingest: %s", filesystem_result.message
                        )
                        if filesystem_result.log_file is not None:
                            logger.warning("log file %s", filesystem_result.log_file)
                    else:
                        processed_results[r.processed_path] = filesystem_result

    if reduction_warnings and not args.force:
        logger.warning(
            "There were reduction warnings! Please check those carefully and then call with --force to apply the "
            "results "
        )
        return 2

    diffs = table_diffractions(dbcontext.metadata, schema=args.main_schema)
    crystals = table_crystals(dbcontext.metadata, schema=args.main_schema)
    data_reduction = table_data_reduction(
        dbcontext.metadata, schema=args.analysis_schema
    )

    with dbcontext.connect() as conn:
        try:
            with conn.begin():
                has_warnings = ingest_diffractions_for_targets(
                    conn, diffs, crystals, targets
                )
                if has_warnings and not args.force:
                    logger.warning(
                        "There were ingestion warnings! Please check those carefully and then call with --force to "
                        "apply the results"
                    )
                    return 3
                has_reduction_warnings = ingest_reductions_for_targets(
                    conn, crystals, data_reduction, targets, UnitRegistry()
                )
                if has_reduction_warnings and not args.force:
                    logger.warning(
                        "There were reduction warnings! Please check those carefully and then call with --force to "
                        "apply the results"
                    )
                    return 4
                if args.dry_run or (
                    has_reduction_warnings or has_warnings and not args.force
                ):
                    raise StopIteration()
                logger.info("Ingested the results!")
        except StopIteration:
            logger.info("Ingested the results as a dry-run")

    return 0 if not warnings else 1


if __name__ == "__main__":
    sys.exit(main())
