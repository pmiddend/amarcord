from extra_data import open_run

from amarcord.amici.xfel.karabo_bridge import Karabo2
from amarcord.amici.xfel.karabo_bridge import KaraboBridgeConfiguration
from amarcord.amici.xfel.karabo_bridge import accumulator_locators_for_config
from amarcord.amici.xfel.karabo_bridge import logger
from amarcord.amici.xfel.karabo_bridge import persist_euxfel_run_result
from amarcord.amici.xfel.karabo_bridge import process_trains
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB


async def extra_data_ingest_run(
    db: AsyncDB, conn: Connection, config: KaraboBridgeConfiguration, run_id: int
) -> None:
    karabo2 = Karabo2(config, first_train_is_start_of_run=True)

    await karabo2.create_missing_attributi(db, conn)

    locators = accumulator_locators_for_config(config)

    euxfel_run = open_run(config.proposal_id, run_id)
    sel = euxfel_run.select(locators)
    trains = sel.trains()
    train_timestamps = euxfel_run.train_timestamps()
    train_ids = euxfel_run.train_ids

    result = process_trains(config, karabo2, run_id, train_ids, trains)

    if result is None:
        logger.error(f"no (successful) trains in run {run_id}")
        return

    await persist_euxfel_run_result(conn, db, result, run_id, train_timestamps)
