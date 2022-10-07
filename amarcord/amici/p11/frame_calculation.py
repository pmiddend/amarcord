from glob import glob
from multiprocessing import Pool
from pathlib import Path
from time import time
from typing import Callable
from typing import Final

import h5py
import structlog

from amarcord.amici.kamzik.kamzik_zmq_client import KAMZIK_ATTRIBUTO_GROUP
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.table_classes import DBRun
from amarcord.experiment_simulator import ATTRIBUTO_TARGET_FRAME_COUNT

logger = structlog.stdlib.get_logger(__name__)


def retrieve_frames_for_data_file(data_file: Path) -> int:
    with h5py.File(data_file, "r") as f:
        return f["entry"]["data"]["data"].shape[0]  # type: ignore


def retrieve_frames_for_raw_file_glob(file_glob: str) -> int:
    result = 0
    for h5_file in glob(file_glob):
        try:
            result += retrieve_frames_for_data_file(Path(h5_file))
        except:
            logger.exception(
                "couldn't get to dimension 1 of entry/data/data, ignoring",
                h5_file=h5_file,
            )
    return result


ATTRIBUTO_RAW_FILES: Final = AttributoId("raw_files")


def update_frames_in_run(attributi: AttributiMap, frames: int) -> None:
    attributi.append_single(ATTRIBUTO_TARGET_FRAME_COUNT, frames)


async def update_runs_add_file_globs(
    db: AsyncDB, conn: Connection, raw_file_glob: Callable[[int], str]
) -> None:
    attributi = await db.retrieve_attributi(conn, AssociatedTable.RUN)
    attributi_dict = {a.name: a for a in attributi}
    raw_file_glob_attributo = attributi_dict.get(ATTRIBUTO_RAW_FILES)
    if raw_file_glob_attributo is None:
        await db.create_attributo(
            conn,
            ATTRIBUTO_RAW_FILES,
            "Glob to the raw files for this run",
            KAMZIK_ATTRIBUTO_GROUP,
            AssociatedTable.RUN,
            AttributoTypeString(),
        )
    for run in await db.retrieve_runs(
        conn, await db.retrieve_attributi(conn, AssociatedTable.RUN)
    ):
        log = logger.bind(run_id=run.id)
        file_glob = raw_file_glob(run.id)
        log.info(f"adding glob {file_glob}")
        run.attributi.append_single(ATTRIBUTO_RAW_FILES, file_glob)
        await db.update_run_attributi(conn, run.id, run.attributi)


def retrieve_run_frames_async(
    run: DBRun,
) -> None | tuple[int, int]:
    log = logger.bind(run_id=run.id)
    file_glob_for_run = run.attributi.select_string(ATTRIBUTO_RAW_FILES)
    if file_glob_for_run is None:
        log.warning("no file glob found (attributo is present though)")
        return None
    frames = retrieve_frames_for_raw_file_glob(file_glob_for_run)
    log.info("{frames} frames")
    return run.id, frames


def retrieve_runs_frames_async(
    run: list[DBRun],
    pool_size: int,
) -> list[tuple[int, int]]:
    with Pool(pool_size) as p:
        return [
            x for x in p.imap_unordered(retrieve_run_frames_async, run) if x is not None
        ]


async def update_run_frames(db: AsyncDB, pool_size: int) -> None:
    # step 1: check if necessary attribute are there and possibly create some
    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(conn, AssociatedTable.RUN)
        attributi_dict = {a.name: a for a in attributi}
        raw_file_glob_attributo = attributi_dict.get(ATTRIBUTO_RAW_FILES)
        assert (
            raw_file_glob_attributo is not None
        ), f'expected an attributo "{raw_file_glob_attributo}" to be present, showing where the files for the run lie; you can create that with "update_runs_add_file_globs" separately'
        if ATTRIBUTO_TARGET_FRAME_COUNT not in attributi_dict:
            await db.create_attributo(
                conn,
                ATTRIBUTO_TARGET_FRAME_COUNT,
                "",
                "external",
                AssociatedTable.RUN,
                AttributoTypeInt(),
            )
        attributi = await db.retrieve_attributi(conn, AssociatedTable.RUN)
        runs = await db.retrieve_runs(conn, attributi)

    # step 2: retrieve the frames per run, in parallel
    begin = time()
    runs_and_frames = retrieve_runs_frames_async(runs, pool_size)

    # step 3: update runs based on the frames found
    async with db.begin() as conn:
        for run_id, frames in runs_and_frames:
            log = logger.bind(run_id=run_id)
            attributi = await db.retrieve_attributi(conn, AssociatedTable.RUN)
            run = await db.retrieve_run(conn, run_id, attributi)
            if run is None:
                continue
            update_frames_in_run(run.attributi, frames)
            log.info(f"detected {frames} frames")
            await db.update_run_attributi(conn, run_id, run.attributi)
    finish = time()
    logger.info(f"updated frames, took {finish - begin}s")
