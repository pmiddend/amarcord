import asyncio
import datetime
import pickle
from pathlib import Path

import yaml

from amarcord.amici.xfel.karabo_bridge import Karabo2
from amarcord.amici.xfel.karabo_bridge import KaraboConfigurationError
from amarcord.amici.xfel.karabo_bridge import ingest_bridge_output
from amarcord.amici.xfel.karabo_bridge import parse_configuration
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata

RUN_CONTROL = "SPB_DAQ_DATA/DM/RUN_CONTROL"


async def run() -> None:
    dbcontext = AsyncDBContext("sqlite+aiosqlite:////tmp/karabo-simulation.db")

    tables = create_tables_from_metadata(dbcontext.metadata)

    db = AsyncDB(dbcontext, tables)

    await db.migrate()

    with Path("../../tests/karabo_online/config.yml").open("r", encoding="utf-8") as f:
        config_file = parse_configuration(yaml.load(f, Loader=yaml.SafeLoader))
        if isinstance(config_file, KaraboConfigurationError):
            raise Exception(config_file)
        karabo2 = Karabo2(config_file)

    async with db.begin() as conn:
        await karabo2.create_missing_attributi(db, conn)

        # Load a basic file (could be middle of a run)
        with (
            Path(__file__).parent.parent.parent
            / "tests"
            / "karabo_online"
            / "events"
            / "1035758364.pickle"
        ).open("rb") as handle:
            dataset_content = pickle.load(handle)
            data = dataset_content["data"]
            metadata = dataset_content["metadata"]
            data[RUN_CONTROL]["proposalNumber.value"] = 1

        # We set the run to have "just started" (train ID equal to the train ID in the data frame)
        data[RUN_CONTROL]["runDetails.firstTrainId.value"] = metadata[RUN_CONTROL][
            "runNumber.value"
        ]["timestamp.tid"]
        data[RUN_CONTROL]["runDetails.length.value"] = 0
        _run_start_time = datetime.datetime(1987, 8, 21, 15, 0, 0, 0)
        data[RUN_CONTROL]["runDetails.beginAt.value"] = "19870821T150000.0Z"
        data[RUN_CONTROL]["runNumber.value"] = 1337

        result = karabo2.process_frame(metadata, data)

        assert result is not None

        await ingest_bridge_output(db, conn, result)


asyncio.run(run())
