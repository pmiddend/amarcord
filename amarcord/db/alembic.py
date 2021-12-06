from alembic import command
from alembic.config import Config


def upgrade_to(url: str, version: str) -> None:
    alembic_cfg = Config()
    # Weird quirk here: alembic is using Python's integrated configparser. SQLAlchemy uses
    # urllib.parse.quote_plus for special characters like '@' in passwords (which will be turned into "%40",
    # for example).
    # configparser uses % as special interpolation syntax, which doesn't mix well. So we replace this by %% instead, see
    # https://stackoverflow.com/questions/37968161/valueerror-in-configparser-in-python3
    alembic_cfg.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    alembic_cfg.set_main_option("script_location", "amarcord:db/migrations")
    command.upgrade(alembic_cfg, version)


def upgrade_to_head(url: str) -> None:
    upgrade_to(url, "head")
