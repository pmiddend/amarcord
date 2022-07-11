import asyncio
from asyncio import FIRST_COMPLETED, Task
from pathlib import Path
from typing import List

from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.kamzik.kamzik_zmq_client import kamzik_main_loop
from amarcord.amici.om.client import OmZMQProcessor
from amarcord.amici.om.simulator import om_simulator_loop
from amarcord.amici.onda.client import OnDAZMQProcessor
from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop
from amarcord.amici.petra3.petra3_online_values import petra3_value_loop
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.tables import create_tables_from_metadata
from amarcord.experiment_simulator import (
    experiment_simulator_initialize_db,
    experiment_simulator_main_loop,
)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    kamzik_socket_url: str | None = None
    kamzik_device_id: str = "Runner"
    petra3_refresh_rate_seconds: float | None = None
    om_url: str | None = None
    om_topic: str = "view:omdata"
    om_simulator_port: int | None = None
    onda_url: str | None = None
    onda_topic: str = "ondadata"
    experiment_simulator_enabled: bool = False
    experiment_simulator_files_dir: Path | None = None
    mjpeg_stream_url: str | None = None
    mjpeg_stream_delay_seconds: float = 5.0


async def _main_loop(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))
    zmq_ctx = Context.instance()
    await db.migrate()

    awaitables: List[Task[None]] = []

    if args.experiment_simulator_enabled:
        await experiment_simulator_initialize_db(db)
        awaitables.append(
            asyncio.create_task(
                experiment_simulator_main_loop(
                    db, args.experiment_simulator_files_dir, delay_seconds=5.0
                )
            )
        )

    if args.mjpeg_stream_url is not None:
        awaitables.append(
            asyncio.create_task(
                mjpeg_stream_loop(
                    db, args.mjpeg_stream_url, args.mjpeg_stream_delay_seconds
                )
            )
        )

    if args.kamzik_socket_url is not None:
        awaitables.append(
            asyncio.create_task(
                kamzik_main_loop(db, args.kamzik_socket_url, args.kamzik_device_id)
            )
        )

    if args.petra3_refresh_rate_seconds is not None:
        awaitables.append(
            asyncio.create_task(petra3_value_loop(db, args.petra3_refresh_rate_seconds))
        )

    if args.om_url is not None:
        processor = OmZMQProcessor(db)
        await processor.init()

        awaitables.append(
            asyncio.create_task(
                processor.main_loop(zmq_ctx, args.om_url, args.om_topic)
            )
        )

    if args.onda_url is not None:
        onda_processor = OnDAZMQProcessor(db)
        await onda_processor.init()

        awaitables.append(
            asyncio.create_task(
                onda_processor.main_loop(zmq_ctx, args.onda_url, args.onda_topic)
            )
        )

    if args.om_simulator_port is not None:
        awaitables.append(
            asyncio.create_task(om_simulator_loop(zmq_ctx, 5.0, args.om_simulator_port))
        )

    await asyncio.wait(
        awaitables,
        return_when=FIRST_COMPLETED,
    )


def main() -> None:
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
