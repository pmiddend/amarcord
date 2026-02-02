import asyncio
import datetime
import shutil
from abc import ABC
from abc import abstractmethod
from enum import Enum
from pathlib import Path

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select
from tap import Tap

from amarcord.db import orm
from amarcord.db.attributo_id import AttributoId
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.event_log_level import EventLogLevel
from amarcord.logging_util import setup_structlog
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    db_connection_url: str
    amarcord_beamtime_id: int
    sshpass_path: str
    esrf_ssh_host: str
    esrf_user: str
    esrf_password: str
    directory_attributo_name: str
    dont_copy_attributo_name: str
    path_prefix: str
    path_prefix_replacement: str
    copy_raw_data: bool = False
    simulate: bool = False
    rsync_path: str = "rsync"
    temp_dir: Path | None = None


class RsyncInterface(ABC):
    @abstractmethod
    async def run_and_wait(
        self, args: Arguments, esrf_glob: str, target: str, additional_args: list[str]
    ) -> int:
        pass  # pragma: no cover


class RealRsyncImplementation(RsyncInterface):  # pragma: no cover
    async def run_and_wait(
        self, args: Arguments, esrf_glob: str, target: str, additional_args: list[str]
    ) -> int:
        real_args = _rsync_args(args, esrf_glob, target, additional_args)

        rsync_process = await asyncio.create_subprocess_exec(
            real_args[0], *real_args[1:]
        )
        await rsync_process.wait()

        assert rsync_process.returncode is not None
        return rsync_process.returncode


class SimulatedRsyncImplementation(RsyncInterface):
    def __init__(self, logger: structlog.stdlib.BoundLogger) -> None:
        self.logger = logger

    async def run_and_wait(
        self, args: Arguments, esrf_glob: str, target: str, additional_args: list[str]
    ) -> int:
        real_args = _rsync_args(args, esrf_glob, target, additional_args)
        self.logger.info("would run rsync with: " + " ".join(real_args))
        shutil.copyfile(esrf_glob, target)
        return 0


def _try_send_event(
    args: Arguments, session: AsyncSession, level: EventLogLevel, message: str
) -> None:
    if level == EventLogLevel.WARNING:
        logger.warning(message)
    elif level == EventLogLevel.ERROR:
        logger.error(message)
    elif level == EventLogLevel.INFO:
        logger.info(message)
    session.add(
        orm.EventLog(
            beamtime_id=BeamtimeId(args.amarcord_beamtime_id),
            level=level,
            source="pull",
            text=message,
            created=datetime.datetime.now(datetime.UTC),
        )
    )


def _rsync_args(
    args: Arguments, esrf_glob: str, target: str, additional_args: list[str]
) -> list[str]:
    return (
        [
            args.rsync_path,
        ]
        + additional_args
        + (
            [
                f'--rsh={args.sshpass_path} -p "{args.esrf_password}" ssh -o StrictHostKeyChecking=no -l {args.esrf_user}'
            ]
            if args.sshpass_path.strip()
            else []
        )
        + [
            "--no-inc-recursive",
            # this leads to a weird sync error
            # "--mkpath",
            "--info=progress2",
            "--info=name0",
            "-avz",
            f"{args.esrf_user}@{args.esrf_ssh_host}:{esrf_glob}",
            target,
        ]
    )


async def _copy_stream_file(
    args: Arguments, session: AsyncSession, ir: orm.IndexingResult
) -> None:
    # This might happen if we index manually in addition, I guess?
    if ir.stream_file is None:
        logger.info(
            f"encountered indexing result without stream file indexing result {ir.id}"
        )
        return

    analysis_output_path = ir.run.beamtime.resolved_analysis_output_path()
    # Must be an already-copied stream file, or a stream file that was
    # created locally instead of remotely, to be copied.
    if ir.stream_file.startswith(analysis_output_path):
        return

    stream_file_locally = Path(
        ir.stream_file.replace(args.path_prefix, args.path_prefix_replacement)
    )

    present_at_desy = stream_file_locally.is_file()
    db_columns_match = str(stream_file_locally) == ir.stream_file

    if present_at_desy and db_columns_match:
        return

    # This can only happen if the file was successfully transferred,
    # because we change the path on the target machine then.
    if present_at_desy and not db_columns_match:
        logger.info(
            f"{stream_file_locally} already exists, but indexing result says it's on ESRF servers ({ir.stream_file}) - correcting"
        )
        ir.stream_file = str(stream_file_locally)
        session.add(ir)
        await session.commit()
        return

    stream_file_locally.parent.mkdir(exist_ok=True, parents=True)
    rsync_implementation = (
        SimulatedRsyncImplementation(logger)
        if args.simulate
        else RealRsyncImplementation()
    )

    _try_send_event(
        args,
        session,
        EventLogLevel.INFO,
        f"copying stream file `{ir.stream_file}` to `{stream_file_locally}` (this might take a while)",
    )
    try:
        rsync_return_code = await rsync_implementation.run_and_wait(
            args,
            esrf_glob=ir.stream_file,
            target=str(stream_file_locally),
            # Compression makes sense for stream files (a lot, actually), but not for h5 files
            additional_args=["--compress", "--compress-level=3"],
        )

        if rsync_return_code != 0:
            _try_send_event(
                args,
                session,
                EventLogLevel.ERROR,
                f"copying `{ir.stream_file}` unsuccessful, check the rsync log file",
            )
            await session.commit()
            return
    except Exception as e:
        _try_send_event(
            args,
            session,
            EventLogLevel.ERROR,
            f"copying `{ir.stream_file}` raised an exception: {e}",
        )
        await session.commit()
        return

    _try_send_event(
        args,
        session,
        EventLogLevel.INFO,
        f"copying `{ir.stream_file}` successful, updating AMARCORD indexing result",
    )

    ir.stream_file = str(stream_file_locally)
    session.add(ir)
    await session.commit()


async def _copy_stream_files(args: Arguments, session: AsyncSession) -> None:
    # A little complicated here, but we cannot select all indexing
    # results from a beam time, since indexing results don't have a
    # beam time in them. They have a reference to a run, and that has
    # a beam time.
    logger.info("=> stream file search starting...")
    for ir in await session.scalars(
        select(orm.IndexingResult)
        .join(orm.Run, orm.IndexingResult.run_id == orm.Run.id)
        .options(selectinload(orm.IndexingResult.run).selectinload(orm.Run.beamtime))
        .options(selectinload(orm.IndexingResult.indexing_parameters))
        .where(orm.Run.beamtime_id == args.amarcord_beamtime_id)
    ):
        await _copy_stream_file(args, session, ir)
    logger.info("<= stream file search ended.")


class CopyResult(Enum):
    COPY_BREAK = "break"
    COPY_CONTINUE = "continue"


async def _copy_single_run(
    args: Arguments,
    session: AsyncSession,
    run: orm.Run,
    dont_copy_id: AttributoId,
    directory_id: AttributoId,
) -> CopyResult:
    if len(run.files) > 0:
        logger.info(f"run {run.id} already copied")
        return CopyResult.COPY_CONTINUE

    run_attributi_by_id = {a.attributo_id: a for a in run.attributo_values}

    dont_copy = run_attributi_by_id.get(dont_copy_id)
    if dont_copy is not None and dont_copy.bool_value:
        logger.info(f"run {run.id}: set to 'do not copy', skipping")
        return CopyResult.COPY_CONTINUE
    remote_run_directory_attributo = run_attributi_by_id.get(directory_id)
    if remote_run_directory_attributo is not None:
        remote_run_directory = remote_run_directory_attributo.string_value
    else:
        remote_run_directory = None

    if remote_run_directory is None:
        logger.error(f"run {run.id}: directory attributo is there, but None, skipping")
        return CopyResult.COPY_CONTINUE

    local_run_directory = remote_run_directory.replace(
        args.path_prefix, args.path_prefix_replacement
    )

    if args.path_prefix not in remote_run_directory:
        logger.warning(
            f"prefix {args.path_prefix} doesn't appear in run directory {remote_run_directory}, skipping"
        )
        return CopyResult.COPY_CONTINUE

    logger.info(f"run {run.id}: {remote_run_directory} => {local_run_directory}")

    rargs = _rsync_args(
        args,
        esrf_glob=f"{remote_run_directory}",
        target=str(local_run_directory),
        additional_args=["--include='*dense*.h5'", "--exclude='*'", "--mkpath"],
    )
    if args.simulate:
        logger.info("simulation: would call rsync with: " + " ".join(rargs))
        logger.info(
            f'simulation: would add "{local_run_directory}*.h5" to run {run.external_id}'
        )
        return CopyResult.COPY_CONTINUE
    try:
        logger.info("to copy h5 files, calling " + " ".join(rargs))
        rsync_process = await asyncio.create_subprocess_exec(rargs[0], *rargs[1:])

        try:
            await asyncio.wait_for(rsync_process.wait(), timeout=5 * 60)
        except TimeoutError:
            logger.info("waited until timeout, continuing with loop")
            rsync_process.terminate()
            await rsync_process.wait()
            return CopyResult.COPY_BREAK

        if rsync_process.returncode != 0:
            _try_send_event(
                args,
                session,
                EventLogLevel.ERROR,
                f"copying `{remote_run_directory}/*.h5` unsuccessful, check the log file",
            )
            await session.commit()
            return CopyResult.COPY_CONTINUE

        run.files.append(
            orm.RunHasFiles(glob=f"{local_run_directory}*.h5", source="raw")
        )
        session.add(run)
        _try_send_event(
            args,
            session,
            EventLogLevel.INFO,
            f"copying `{remote_run_directory}/*.h5` successful",
        )
        await session.commit()
        return CopyResult.COPY_BREAK
    except Exception as e:
        logger.exception(f"copying unsuccesful: {e}")
        # _try_send_event(
        #     args,
        #     session,
        #     EventLogLevel.INFO,
        #     f"copying `{remote_run_directory}/*.h5` unsuccessful, check logs",
        # )
        await session.commit()
        return CopyResult.COPY_CONTINUE


async def _copy_raw_data(args: Arguments, session: AsyncSession) -> None:
    attributi_by_name = {
        a.name: a
        for a in await session.scalars(
            select(orm.Attributo).where(
                orm.Attributo.beamtime_id == args.amarcord_beamtime_id
            )
        )
    }
    directory_attributo = attributi_by_name.get(args.directory_attributo_name)
    if directory_attributo is None:
        logger.error(
            f"cannot find directory attributo {args.directory_attributo_name}, check the pull daemon's config; attributi names are "
            + " ,".join(attributi_by_name.keys())
        )
        return
    dont_copy_attributo = attributi_by_name.get(args.dont_copy_attributo_name)
    if dont_copy_attributo is None:
        logger.error(
            f'cannot find "do not copy" attributo {args.dont_copy_attributo_name}, check the pull daemon\'s config; attributi names are '
            + " ,".join(attributi_by_name.keys())
        )
        return

    for run in await session.scalars(
        select(orm.Run)
        .where(orm.Run.beamtime_id == args.amarcord_beamtime_id)
        .options(selectinload(orm.Run.files))
    ):
        single_run_result = await _copy_single_run(
            args, session, run, dont_copy_attributo.id, directory_attributo.id
        )
        if single_run_result == CopyResult.COPY_BREAK:
            break


async def _main_loop_iteration(args: Arguments, session: AsyncSession) -> None:
    if args.copy_raw_data:
        await _copy_raw_data(args, session)
    await _copy_stream_files(args, session)


async def _async_main(args: Arguments) -> None:
    async_session = get_orm_sessionmaker_with_url(args.db_connection_url)
    async with async_session() as session:
        await _main_loop_iteration(args, session)


def main() -> None:
    asyncio.run(_async_main(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":  # pragma: no cover
    main()
