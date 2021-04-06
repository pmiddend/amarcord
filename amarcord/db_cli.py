import argparse
import logging
import sys

from amarcord.db.alembic import upgrade_to_head

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


def main() -> int:
    parser = argparse.ArgumentParser(prog="AMARCORD db_cli")
    parser.add_argument("--connection-url", type=str, required=True)

    subparsers = parser.add_subparsers(dest="command")

    _migrate_parser = subparsers.add_parser("migrate")
    args = parser.parse_args()

    if args.command == "migrate":
        upgrade_to_head(args.connection_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
