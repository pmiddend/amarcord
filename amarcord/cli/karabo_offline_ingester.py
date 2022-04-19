import asyncio
import logging
from pathlib import Path

import yaml
from tap import Tap

from amarcord.amici.xfel.karabo_bridge import (
    parse_configuration,
    KaraboConfigurationError,
    extra_data_ingest_run,
    KaraboBridgeConfiguration,
)
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    karabo_config_file: Path  # Karabo configuration file; if given, will enable the Karabo daemon
    run_id: int

    """AMARCORD XFEL EXtra-data ingest daemon"""


def main() -> None:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = AsyncDBContext(args.db_connection_url)

    tables = create_tables_from_metadata(dbcontext.metadata)

    db = AsyncDB(dbcontext, tables)

    asyncio.run(db.migrate())

    with args.karabo_config_file.open("r") as f:
        config_file = parse_configuration(yaml.load(f, Loader=yaml.SafeLoader))
        if isinstance(config_file, KaraboConfigurationError):
            raise Exception(config_file)

    asyncio.run(async_main(db, config_file, args.run_id))


async def async_main(
    db: AsyncDB, config_file: KaraboBridgeConfiguration, run_id: int
) -> None:
    async with db.begin() as conn:
        await extra_data_ingest_run(db, conn, config_file, run_id)


if __name__ == "__main__":
    main()
