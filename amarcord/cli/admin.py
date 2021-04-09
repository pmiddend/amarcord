import argparse
import logging
import sys
from getpass import getpass

from amarcord.db.alembic import upgrade_to_head
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(prog="AMARCORD db_cli")
    parser.add_argument("--connection-url", type=str, required=True)

    subparsers = parser.add_subparsers(dest="command")

    switch_admin_password = subparsers.add_parser("switch-admin-password")
    switch_admin_password.add_argument("--proposal-id", type=int, required=True)

    remove_admin_password = subparsers.add_parser("remove-admin-password")
    remove_admin_password.add_argument("--proposal-id", type=int, required=True)

    add_proposal = subparsers.add_parser("add-proposal")
    add_proposal.add_argument("--proposal-id", type=int, required=True)
    add_proposal.add_argument("--proposal-password", type=str, required=False)

    _migrate_parser = subparsers.add_parser("migrate")
    args = parser.parse_args()

    dbcontext = DBContext(args.connection_url)
    db = DB(dbcontext, create_tables(dbcontext))
    if args.command == "migrate":
        upgrade_to_head(args.connection_url)
        logger.info("Migration finished successfully!")
    elif args.command == "remove-admin-password":
        with db.connect() as conn:
            db.change_proposal_password(conn, args.proposal_id, None)
            logger.info("Password successfully removed!")
    elif args.command == "switch-admin-password":
        new_password = getpass()
        with db.connect() as conn:
            db.change_proposal_password(conn, args.proposal_id, new_password)
            logger.info("Password successfully switched!")
    elif args.command == "add-proposal":
        with db.connect() as conn:
            db.add_proposal(conn, ProposalId(args.proposal_id), args.proposal_password)
            logger.info("Proposal added!")
    else:
        logger.warning("No command given!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
