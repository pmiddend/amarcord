import logging
import sys
from pathlib import Path
from typing import List
from typing import Optional

from tap import Tap

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


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    folders: List[
        str
    ]  # Folders to process (have to contain stream files and a parameters.json file)
    run_id: int  # Run ID to ingest for
    stream_pattern: str = "*.stream*"  # "Pattern for matching stream filenames"
    harvest_filename: str = "parameters.json"
    proposal_id: int  # ID of the proposal to ingest data for
    force_run_creation: bool = (
        False  # Create new runs if they don't exist in the DB yet
    )
    dry_run: bool = False
    tag: Optional[str] = None

    """Cheetah output to AMARCORD DB ingester"""


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    if not Path(args.harvest_filename).exists():
        sys.stderr.write(f"harvest filename {args.harvest_filename} doesn't exist!\n")
        sys.exit(1)

    dbcontext = DBContext(args.db_connection_url, echo=bool(args.dry_run))
    tables = create_tables(dbcontext)
    if args.db_connection_url.startswith("sqlite://"):
        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    db = DB(dbcontext, tables)

    proposal_id = ProposalId(args.proposal_id)

    if args.db_connection_url.startswith("sqlite://"):
        # Just for testing!
        with db.dbcontext.connect() as local_conn:
            if not db.have_proposals(local_conn):
                db.add_proposal(local_conn, proposal_id)
    for folder in args.folders:
        if not Path(folder).is_dir():
            logger.error("The directory %s is not a directory", folder)
            sys.exit(1)
        try:
            with db.connect() as conn:
                with conn.begin():
                    run_ids = set(db.retrieve_run_ids(conn, proposal_id))
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
                            args.force_run_creation,
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
