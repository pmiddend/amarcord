import argparse
import logging

from amarcord.db.alembic import upgrade_to_head

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

parser = argparse.ArgumentParser(prog="AMARCORD db_cli")
parser.add_argument("--connection-url", type=str, required=True)

subparsers = parser.add_subparsers(dest="command")

migrate_parser = subparsers.add_parser("migrate")
args = parser.parse_args()

if args.command == "migrate":
    upgrade_to_head(args.connection_url)
