import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

from amarcord.amici.crystfel.injest import harvest_folder
from amarcord.amici.crystfel.injest import ingest_data_source
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class DryRunException(Exception):
    pass


def main() -> int:
    parser = ArgumentParser(description="Ingest indexing data for AMARCORD")

    parser.add_argument(
        metavar="folder",
        nargs="*",
        type=str,
        default=".",
        help="Folder to process",
        dest="folder",
    )
    parser.add_argument(
        "--stream-pattern",
        type=str,
        default="*.stream*",
        help="Pattern for matching stream filenames",
    )
    parser.add_argument(
        "--harvest-filename",
        type=str,
        default="parameters.json",
        help="Name of harvest file",
    )
    parser.add_argument(
        "--connection-url", type=str, help="Database URL", required=True
    )
    parser.add_argument(
        "--proposal-id",
        type=int,
        help="Proposal ID to search for analysis results in",
    )
    parser.add_argument("--run-id", type=int, help="Run ID", required=True)
    parser.add_argument(
        "--force-create-run",
        action="store_true",
        help="Should we create runs that don't exist?",
        required=False,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't change the DB, output statements",
        required=False,
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Tag the results",
        required=False,
    )
    args = parser.parse_args()

    if not Path(args.harvest_filename).exists():
        sys.stderr.write(f"harvest filename {args.harvest_filename} doesn't exist!\n")
        sys.exit(1)

    dbcontext = DBContext(args.connection_url, echo=bool(args.dry_run))
    tables = create_tables(dbcontext)
    if args.connection_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    db = DB(dbcontext, tables)

    if args.connection_url.startswith("sqlite://"):
        # Just for testing!
        with db.dbcontext.connect() as local_conn:
            if not db.have_proposals(local_conn):
                db.add_proposal(local_conn, args.proposal_id)
    for folder in args.folder:
        if not Path(folder).is_dir():
            logger.error("The directory %s is not a directory", folder)
            sys.exit(1)
        try:
            with db.connect() as conn:
                with conn.begin():
                    run_ids = set(db.retrieve_run_ids(conn, args.proposal_id))
                    updated_data_sources = harvest_folder(
                        db.retrieve_analysis_data_sources(conn),
                        Path(folder),
                        args.run_id,
                        args.stream_pattern,
                        Path(args.harvest_filename),
                        args.tag,
                    )
                    for ds in updated_data_sources:
                        for event in ingest_data_source(
                            db,
                            conn,
                            args.force_create_run,
                            ProposalId(args.proposal_id),
                            run_ids,
                            ds,
                        ):
                            db.add_event(
                                conn,
                                event.level,
                                event.source,
                                event.text,
                                event.created,
                            )
                    logger.info(f"updated data sources: {updated_data_sources}")
                    if args.dry_run:
                        raise DryRunException()
        except DryRunException:
            continue

    # noinspection PyUnreachableCode
    return 0


if __name__ == "__main__":
    sys.exit(main())
