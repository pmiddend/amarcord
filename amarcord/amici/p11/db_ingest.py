import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import sqlalchemy as sa

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.run_key import RunKey
from amarcord.modules.dbcontext import Connection
from amarcord.newdb.beamline import Beamline
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.newdb import NewDB
from amarcord.util import path_mtime
from amarcord.xtal_util import find_space_group_index_by_name

EIGER_16_M_DETECTOR_NAME = "DECTRIS EIGER 16M"

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetadataRetriever:
    """
    This "Metadata Retriever" solves the problem that the diffractions we have on the filesystem do not have "user
    metadata" attached to them, like "diffraction outcome" or "comment". If we want to ingest that sort of data after
    the fact (i.e. not during the experiment), we need a way to "back-fill" these values. This is what the metadata
    retriever does.
    """

    diffraction: Callable[[str, int], DiffractionType]
    comment: Callable[[str, int], str]
    estimated_resolution: Callable[[str, int], str]
    detector_name: str


def empty_metadata_retriever(detector_name: str) -> MetadataRetriever:
    """
    An empty metadata retriever, which assumes every diffraction is a success and has no comment.
    """
    return MetadataRetriever(
        lambda cid, rid: DiffractionType.success,
        lambda cid, rid: "",
        lambda cid, rid: "",
        detector_name,
    )


def _find_crystal(
    conn: Connection, crystals: sa.Table, puck_id: str, position: int
) -> Optional[str]:
    crystal_id_row = conn.execute(
        sa.select([crystals.c.crystal_id]).where(
            sa.and_(
                crystals.c.puck_id == puck_id,
                crystals.c.puck_position_id == position,
            )
        )
    ).fetchone()

    return crystal_id_row[0] if crystal_id_row is not None else None


def _ingest_diffractions_for_crystal(
    conn: Connection,
    db: NewDB,
    crystal: P11Crystal,
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> List[str]:
    if not db.crystal_exists(conn, crystal.crystal_id):
        return [
            f"crystal {crystal.crystal_id} found in filesystem, but not in database"
        ]

    warnings: List[str] = []
    for run in crystal.runs:
        if (
            not db.has_diffractions(conn, crystal.crystal_id, run.run_id)
            and not insert_diffraction_if_not_exists
        ):
            logger.debug(
                f"diffraction for crystal ID {crystal.crystal_id}, run ID {run.run_id} does not exist, not adding it"
            )
            continue
        logger.info(
            "Crystal ID %s, run %s, ingesting diffraction...",
            crystal.crystal_id,
            run.run_id,
        )
        _ingest_diffraction(
            conn,
            db,
            crystal.crystal_id,
            run,
            insert_diffraction_if_not_exists,
            metadata_retriever,
        )

    return warnings


def _ingest_diffraction(
    conn: Connection,
    db: NewDB,
    crystal_id: str,
    run: P11Run,
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> None:
    if not insert_diffraction_if_not_exists:
        update_diffraction(conn, db, crystal_id, run)
    else:
        exists = db.has_diffractions(conn, crystal_id, run.run_id)

        if exists:
            update_diffraction(conn, db, crystal_id, run)
        else:
            insert_diffraction(conn, db, crystal_id, run, metadata_retriever)


def insert_diffraction(
    conn: Connection,
    db: NewDB,
    crystal_id: str,
    run: P11Run,
    metadata_retriever: MetadataRetriever,
) -> None:
    db.insert_diffraction(
        conn,
        DBDiffraction(
            crystal_id=crystal_id,
            run_id=run.run_id,
            pinhole=run.info_file.aperture.magnitude,
            focusing=run.info_file.focus,
            metadata=crystal_id,
            diffraction=metadata_retriever.diffraction(crystal_id, run.run_id),  # type: ignore
            comment=metadata_retriever.comment(crystal_id, run.run_id),  # type: ignore
            estimated_resolution=metadata_retriever.estimated_resolution(crystal_id, run.run_id),  # type: ignore
            angle_start=run.info_file.start_angle.to("deg").magnitude,
            number_of_frames=run.info_file.frames,
            angle_step=run.info_file.degrees_per_frame.to("deg").magnitude,
            exposure_time=run.info_file.exposure_time.to("millisecond").magnitude,
            xray_wavelength=run.info_file.wavelength.to("angstrom").magnitude,
            detector_distance=run.info_file.detector_distance.to(
                "millimeter"
            ).magnitude,
            detector_edge_resolution=run.info_file.resolution.to("angstrom").magnitude,
            detector_name=metadata_retriever.detector_name,
            aperture_radius=run.info_file.aperture.to("micrometer").magnitude,
            filter_transmission=run.info_file.filter_transmission_percent,
            ring_current=run.info_file.ring_current.to("milliampere").magnitude,
            data_raw_filename_pattern=Path(run.data_raw_filename_pattern)
            if run.data_raw_filename_pattern is not None
            else None,
            created=path_mtime(Path(run.data_raw_filename_pattern).parent)
            if run.data_raw_filename_pattern
            else datetime.datetime.now(),
            microscope_image_filename_pattern=Path(
                run.microscope_image_filename_pattern
            )
            if run.microscope_image_filename_pattern is not None
            else None,
        ),
    )


def update_diffraction(
    conn: Connection, db: NewDB, crystal_id: str, run: P11Run
) -> None:
    db.update_diffraction(
        conn,
        DBDiffraction(
            crystal_id=crystal_id,
            run_id=run.run_id,
            pinhole=run.info_file.aperture.magnitude,
            focusing=run.info_file.focus,
            metadata=crystal_id,
            angle_start=run.info_file.start_angle.to("deg").magnitude,
            number_of_frames=run.info_file.frames,
            angle_step=run.info_file.degrees_per_frame.to("deg").magnitude,
            exposure_time=run.info_file.exposure_time.to("millisecond").magnitude,
            xray_wavelength=run.info_file.wavelength.to("angstrom").magnitude,
            detector_distance=run.info_file.detector_distance.to(
                "millimeter"
            ).magnitude,
            detector_edge_resolution=run.info_file.resolution.to("angstrom").magnitude,
            detector_name=EIGER_16_M_DETECTOR_NAME,
            aperture_radius=run.info_file.aperture.to("micrometer").magnitude,
            filter_transmission=run.info_file.filter_transmission_percent,
            ring_current=run.info_file.ring_current.to("milliampere").magnitude,
            data_raw_filename_pattern=Path(run.data_raw_filename_pattern)
            if run.data_raw_filename_pattern is not None
            else None,
            microscope_image_filename_pattern=Path(
                run.microscope_image_filename_pattern
            )
            if run.microscope_image_filename_pattern is not None
            else None,
            # In this case means: don't update it
            diffraction=None,
            beamline=Beamline.p11,
        ),
    )


def ingest_diffractions_for_crystals(
    conn: Connection,
    db: NewDB,
    crystals: List[P11Crystal],
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> List[str]:
    warnings: List[str] = []
    for crystal in crystals:
        this_has_warnings = _ingest_diffractions_for_crystal(
            conn,
            db,
            crystal,
            insert_diffraction_if_not_exists,
            metadata_retriever,
        )
        warnings.extend(this_has_warnings)
    return warnings


def ingest_reductions_for_crystals(
    conn: Connection,
    db: NewDB,
    crystals: List[P11Crystal],
    processed_results: Dict[RunKey, List[AnalysisResult]],
) -> List[str]:
    warnings: List[str] = []
    for crystal in crystals:
        for run in crystal.runs:
            process_results = processed_results.get(
                RunKey(crystal.crystal_id, run.run_id), None
            )

            if process_results is None or not processed_results:
                logger.debug(
                    "Crystal %s, skipping run %s, no process result",
                    crystal.crystal_id,
                    run.run_id,
                )
                continue

            if not db.has_diffractions(conn, crystal.crystal_id, run.run_id):
                logger.debug(
                    f"crystal {crystal.crystal_id}, run {run.run_id}: cannot ingest data reduction: got no "
                    f"corresponding diffraction image "
                )
                continue

            for process_result in process_results:
                if db.directory_has_reductions(conn, process_result.base_path):
                    logger.debug(
                        "Data reduction for folder %s already exists",
                        process_result.base_path,
                    )
                    continue

                logger.info(
                    "Crystal %s, run %s, processed path %s: ingesting %s result",
                    crystal.crystal_id,
                    run.run_id,
                    process_result.base_path,
                    process_result.method.value,
                )
                db.insert_data_reduction(
                    conn,
                    DBDataReduction(
                        data_reduction_id=None,
                        crystal_id=crystal.crystal_id,
                        run_id=run.run_id,
                        analysis_time=process_result.analysis_time,
                        folder_path=process_result.base_path,
                        mtz_path=process_result.mtz_file,
                        method=process_result.method,
                        resolution_cc=process_result.resolution_cc,
                        resolution_isigma=process_result.resolution_isigma,
                        a=process_result.a,
                        b=process_result.b,
                        c=process_result.c,
                        alpha=process_result.alpha,
                        beta=process_result.beta,
                        gamma=process_result.gamma,
                        space_group=find_space_group_index_by_name(
                            process_result.space_group
                        )
                        if isinstance(process_result.space_group, str)
                        else process_result.space_group,
                        isigi=process_result.isigi,
                        rmeas=process_result.rmeas,
                        cchalf=process_result.cchalf,
                        rfactor=process_result.rfactor,
                        wilson_b=process_result.wilson_b,
                    ),
                )
    return warnings
