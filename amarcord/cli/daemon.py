import asyncio
from asyncio import FIRST_COMPLETED
from asyncio import Task
from pathlib import Path
from typing import Optional

from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.crystfel.indexing_daemon import CrystFELOnlineConfig
from amarcord.amici.crystfel.indexing_daemon import indexing_loop
from amarcord.amici.crystfel.merge_daemon import MergeConfig
from amarcord.amici.crystfel.merge_daemon import merging_loop
from amarcord.amici.kamzik.kamzik_zmq_client import kamzik_main_loop
from amarcord.amici.om.client import OmZMQProcessor
from amarcord.amici.om.indexing_daemon import om_indexing_loop
from amarcord.amici.om.simulator import om_simulator_loop
from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,  # NOQA
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,  # NOQA
)
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributo_id import AttributoId
from amarcord.db.tables import create_tables_from_metadata
from amarcord.experiment_simulator import experiment_simulator_initialize_db
from amarcord.experiment_simulator import experiment_simulator_main_loop
from amarcord.logging_util import setup_structlog

setup_structlog()

class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    # pylint: disable=consider-alternative-union-syntax
    kamzik_socket_url: Optional[str] = None
    kamzik_device_id: str = "Runner"
    # pylint: disable=consider-alternative-union-syntax
    petra3_refresh_rate_seconds: Optional[float] = None
    # pylint: disable=consider-alternative-union-syntax
    om_url: Optional[str] = None
    om_topic: str = "view:omdata"
    # pylint: disable=consider-alternative-union-syntax
    om_simulator_port: Optional[int] = None
    # pylint: disable=consider-alternative-union-syntax
    om_indexing_file: Optional[Path] = None  # Whether to enable the Om indexing daemon
    experiment_simulator_enabled: bool = False
    # pylint: disable=consider-alternative-union-syntax
    experiment_simulator_files_dir: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    mjpeg_stream_url: Optional[str] = None
    mjpeg_stream_delay_seconds: float = 5.0
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_output_base_directory: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_cell_file_path: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_indexing_script_path: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_chemical_attributo: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_workload_manager_uri: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    merge_daemon_workload_manager_uri: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    merge_daemon_output_base_directory: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    merge_daemon_api_url: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    merge_daemon_crystfel_path: Optional[Path] = None


async def _main_loop(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))
    zmq_ctx = Context.instance()
    await db.migrate()

    awaitables: list[Task[None]] = []

    if (
        args.merge_daemon_workload_manager_uri is not None
        and args.merge_daemon_crystfel_path is not None
        and args.merge_daemon_output_base_directory is not None
        and args.merge_daemon_api_url is not None
    ):
        awaitables.append(
            asyncio.create_task(
                merging_loop(
                    db=db,
                    config=MergeConfig(
                        output_base_directory=args.merge_daemon_output_base_directory,
                        api_url=args.merge_daemon_api_url,
                        crystfel_path=args.merge_daemon_crystfel_path,
                    ),
                    workload_manager=create_workload_manager(
                        parse_workload_manager_config(
                            args.merge_daemon_workload_manager_uri
                        )
                    ),
                )
            )
        )

    if (
        args.online_crystfel_output_base_directory is not None
        and args.online_crystfel_indexing_script_path is not None
        and args.online_crystfel_workload_manager_uri is not None
        and args.online_crystfel_chemical_attributo is not None
        and args.online_crystfel_cell_file_path is not None
    ):
        awaitables.append(
            asyncio.create_task(
                indexing_loop(
                    db=db,
                    workload_manager=create_workload_manager(
                        parse_workload_manager_config(
                            args.online_crystfel_workload_manager_uri
                        )
                    ),
                    config=CrystFELOnlineConfig(
                        args.online_crystfel_output_base_directory,
                        args.online_crystfel_cell_file_path,
                        args.online_crystfel_indexing_script_path,
                        AttributoId(args.online_crystfel_chemical_attributo),
                    ),
                )
            )
        )

    if args.om_indexing_file is not None:
        awaitables.append(
            asyncio.create_task(
                om_indexing_loop(
                    db,
                    watch_file=args.om_indexing_file,
                )
            )
        )

    if args.experiment_simulator_enabled:
        await experiment_simulator_initialize_db(db)
        awaitables.append(
            asyncio.create_task(
                experiment_simulator_main_loop(
                    db,
                    delay_seconds=5.0,
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

    if args.om_url is not None:
        processor = OmZMQProcessor(db)
        awaitables.append(
            asyncio.create_task(
                processor.main_loop(zmq_ctx, args.om_url, args.om_topic)
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
