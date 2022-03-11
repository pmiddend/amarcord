import asyncio
from asyncio import FIRST_COMPLETED
from pathlib import Path
from typing import Optional, List, Awaitable

from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.kamzik.kamzik_zmq_client import kamzik_main_loop
from amarcord.amici.om.client import OmZMQProcessor
from amarcord.amici.om.simulator import om_simulator_loop
from amarcord.amici.petra3.petra3_online_values import petra3_value_loop
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata
from amarcord.experiment_simulator import (
    experiment_simulator_initialize_db,
    experiment_simulator_main_loop,
)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    kamzik_socket_url: Optional[str] = None
    kamzik_device_id: str = "Runner"
    petra3_refresh_rate_seconds: Optional[float] = None
    om_url: Optional[str] = None
    om_topic: str = "view:omdata"
    om_simulator_port: Optional[int] = None
    experiment_simulator_enabled: bool = False
    experiment_simulator_files_dir: Optional[Path] = None


async def _main_loop(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))
    zmq_ctx = Context.instance()
    await db_context.create_all(CreationMode.CHECK_FIRST)

    awaitables: List[Awaitable[None]] = []

    if args.experiment_simulator_enabled:
        await experiment_simulator_initialize_db(db)
        awaitables.append(
            experiment_simulator_main_loop(
                db, args.experiment_simulator_files_dir, delay_seconds=5.0
            )
        )

    if args.kamzik_socket_url is not None:
        awaitables.append(
            kamzik_main_loop(db, args.kamzik_socket_url, args.kamzik_device_id)
        )

    if args.petra3_refresh_rate_seconds is not None:
        awaitables.append(petra3_value_loop(db, args.petra3_refresh_rate_seconds))

    if args.om_url is not None:
        processor = OmZMQProcessor(db)
        await processor.init()

        awaitables.append(processor.main_loop(zmq_ctx, args.om_url, args.om_topic))

    if args.om_simulator_port is not None:
        awaitables.append(om_simulator_loop(zmq_ctx, 5.0, args.om_simulator_port))

    await asyncio.wait(
        awaitables,
        return_when=FIRST_COMPLETED,
    )


if __name__ == "__main__":
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))
