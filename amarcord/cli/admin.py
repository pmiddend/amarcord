import argparse
import logging
import sys
from getpass import getpass

from amarcord.db.alembic import upgrade_to_head
from amarcord.db.db import DB
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="AMARCORD db_cli")
    parser.add_argument("--connection-url", type=str, required=True)

    subparsers = parser.add_subparsers(dest="command")

    switch_admin_password = subparsers.add_parser("switch-admin-password")
    switch_admin_password.add_argument("--proposal_id", type=int, required=True)

    remove_admin_password = subparsers.add_parser("remove-admin-password")
    remove_admin_password.add_argument("--proposal_id", type=int, required=True)

    _migrate_parser = subparsers.add_parser("migrate")
    args = parser.parse_args()

    dbcontext = DBContext(args.connection_url)
    db = DB(dbcontext, create_tables(dbcontext))
    if args.command == "migrate":
        upgrade_to_head(args.connection_url)
    if args.command == "remove-admin-password":
        with db.connect() as conn:
            db.change_proposal_password(conn, args.proposal_id, None)
    if args.command == "switch-admin-password":
        new_password = getpass()
        with db.connect() as conn:
            db.change_proposal_password(conn, args.proposal_id, new_password)
    return 0


if __name__ == "__main__":
    sys.exit(main())
