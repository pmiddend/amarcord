from alembic import command
from alembic.config import Config


def upgrade_to(url: str, version: str) -> None:
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    alembic_cfg.set_main_option("script_location", "amarcord:amici/p11/migrations")
    command.upgrade(alembic_cfg, version)


def upgrade_to_head(url: str) -> None:
    upgrade_to(url, "head")
