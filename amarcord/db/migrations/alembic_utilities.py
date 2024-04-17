import argparse
import itertools

from alembic import command
from alembic.config import Config
from sqlalchemy import Connection


def upgrade_to_connection(
    conn: Connection, version: str, additional_args: dict[str, str] | None = None
) -> None:
    alembic_cfg = Config()
    # see https://alembic.sqlalchemy.org/en/latest/cookbook.html#programmatic-api-use-connection-sharing-with-asyncio
    # pylint: disable=unsupported-assignment-operation
    alembic_cfg.attributes["connection"] = conn
    alembic_cfg.set_main_option("script_location", "amarcord:db/migrations")
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-x", action="append")
    # Horribly unsafe and badly written, but please somebody show me how to do this right? How do I
    # pass -x parameters programmatically to alembic?
    alembic_cfg.cmd_opts = argparser.parse_args(
        list(
            itertools.chain.from_iterable(
                [["-x", f"{key}={value}"] for key, value in additional_args.items()]
            )
        )
        if additional_args is not None
        else []
    )
    command.upgrade(alembic_cfg, version)


def upgrade_to_head_connection(
    connection: Connection, additional_args: dict[str, str] | None = None
) -> None:
    upgrade_to_connection(connection, "head", additional_args)
