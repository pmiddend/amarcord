import asyncio
from asyncio import FIRST_COMPLETED
from asyncio import Task
from pathlib import Path
from typing import Optional

from tap import Tap

from amarcord.amici.crystfel.indexing_daemon import CrystFELOnlineConfig
from amarcord.amici.crystfel.indexing_daemon import indexing_loop
from amarcord.amici.crystfel.merge_daemon import MergeConfig
from amarcord.amici.crystfel.merge_daemon import merging_loop
from amarcord.amici.p11.grab_mjpeg_frame import mjpeg_stream_loop
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,  # NOQA
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,  # NOQA
)
from amarcord.cli.experiment_simulator import experiment_simulator_initialize_db
from amarcord.cli.experiment_simulator import experiment_simulator_main_loop
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.tables import create_tables_from_metadata
from amarcord.logging_util import setup_structlog

setup_structlog()


class Arguments(Tap):
    db_connection_url: (
        str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    )
    experiment_simulator_enabled: bool = False
    # pylint: disable=consider-alternative-union-syntax
    mjpeg_stream_url: Optional[str] = None
    mjpeg_stream_delay_seconds: float = 5.0
    # pylint: disable=consider-alternative-union-syntax
    mjpeg_stream_beamtime_id: Optional[int] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_output_base_directory: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_crystfel_path: Optional[Path] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_use_auto_geom_refinement: bool = False
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_api_url: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_dummy_h5_input: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_beamtime_id: Optional[int] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_workload_manager_uri: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_asapo_source: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    online_crystfel_cpu_count_multiplier: Optional[float] = None
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
    await db.migrate()

    awaitables: list[Task[None]] = []

    if (
        args.merge_daemon_workload_manager_uri is not None
        and args.merge_daemon_crystfel_path is not None
        and args.merge_daemon_api_url is not None
        and args.merge_daemon_output_base_directory is not None
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
        and args.online_crystfel_crystfel_path is not None
        and args.online_crystfel_workload_manager_uri is not None
        and args.online_crystfel_asapo_source is not None
        and args.online_crystfel_api_url is not None
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
                        (
                            BeamtimeId(args.online_crystfel_beamtime_id)
                            if args.online_crystfel_beamtime_id is not None
                            else None
                        ),
                        args.online_crystfel_crystfel_path,
                        args.online_crystfel_api_url,
                        args.online_crystfel_asapo_source,
                        args.online_crystfel_cpu_count_multiplier,
                        args.online_crystfel_use_auto_geom_refinement,
                        args.online_crystfel_dummy_h5_input,
                    ),
                )
            )
        )

    if args.experiment_simulator_enabled:
        simulator_data = await experiment_simulator_initialize_db(db)
        awaitables.append(
            asyncio.create_task(
                experiment_simulator_main_loop(
                    db, delay_seconds=5.0, simulator_data=simulator_data
                )
            )
        )

    if args.mjpeg_stream_url is not None:
        if args.mjpeg_stream_beamtime_id is None:
            raise Exception(
                "got an mjpeg stream URL but no beamtime ID - old daemon configuration?"
            )
        awaitables.append(
            asyncio.create_task(
                mjpeg_stream_loop(
                    db,
                    args.mjpeg_stream_url,
                    args.mjpeg_stream_delay_seconds,
                    args.mjpeg_stream_beamtime_id,
                )
            )
        )

    await asyncio.wait(
        awaitables,
        return_when=FIRST_COMPLETED,
    )


def main() -> None:
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
