from alembic import command
from alembic.config import Config


def upgrade_to_head(url: str) -> None:
    alembic_cfg = Config()
    alembic_cfg.set_main_option("sqlalchemy.url", url)
    alembic_cfg.set_main_option("script_location", "amarcord:db/migrations")
    command.upgrade(alembic_cfg, "head")
