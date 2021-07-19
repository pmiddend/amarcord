import datetime
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import Table

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.analyze_filesystem import P11Target
from amarcord.amici.p11.run_key import RunKey
from amarcord.modules.dbcontext import Connection
from amarcord.newdb.db import DiffractionType
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
    diffs: sa.Table,
    crystals: sa.Table,
    crystal: P11Crystal,
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> List[str]:
    if (
        conn.execute(
            sa.select([crystals.c.crystal_id]).where(
                crystals.c.crystal_id == crystal.crystal_id
            )
        ).fetchone()
        is None
    ):
        return [
            f"crystal {crystal.crystal_id} found in filesystem, but not in database"
        ]

    warnings: List[str] = []
    for run in crystal.runs:
        if (
            conn.execute(
                sa.select([diffs.c.crystal_id]).where(
                    sa.and_(
                        diffs.c.crystal_id == crystal.crystal_id,
                        diffs.c.run_id == run.run_id,
                    )
                )
            ).fetchone()
            is None
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
            crystal.crystal_id,
            diffs,
            run,
            insert_diffraction_if_not_exists,
            metadata_retriever,
        )

    return warnings


def _ingest_diffractions_for_target(
    conn: Connection,
    diffs: sa.Table,
    crystals: sa.Table,
    target: P11Target,
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> bool:
    has_warnings = False
    for puck in target.pucks:
        crystal_id = _find_crystal(conn, crystals, puck.puck_id, puck.position)

        if crystal_id is None:
            logger.warning(
                "found no crystal for puck ID %s and position %s",
                puck.puck_id,
                puck.position,
            )
            has_warnings = True
            continue

        for run in puck.runs:
            if (
                conn.execute(
                    sa.select([diffs.c.crystal_id]).where(
                        sa.and_(
                            diffs.c.crystal_id == crystal_id,
                            diffs.c.run_id == run.run_id,
                        )
                    )
                ).fetchone()
                is not None
            ):
                logger.info(
                    "Diffraction for crystal ID %s, run ID %s exists",
                    crystals,
                    run.run_id,
                )
                continue
            _ingest_diffraction(
                conn,
                crystal_id,
                diffs,
                run,
                insert_diffraction_if_not_exists,
                metadata_retriever,
            )

    return has_warnings


def _ingest_diffraction(
    conn: Connection,
    crystal_id: str,
    diffs: sa.Table,
    run: P11Run,
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> None:
    if not insert_diffraction_if_not_exists:
        update_diffraction(conn, crystal_id, diffs, run)
    else:
        exists = conn.execute(
            sa.select([diffs.c.run_id]).where(
                sa.and_(diffs.c.run_id == run.run_id, diffs.c.crystal_id == crystal_id)
            )
        ).fetchall()

        if exists:
            update_diffraction(conn, crystal_id, diffs, run)
        else:
            insert_diffraction(conn, crystal_id, diffs, run, metadata_retriever)


def insert_diffraction(
    conn: Connection,
    crystal_id: str,
    diffs: sa.Table,
    run: P11Run,
    metadata_retriever: MetadataRetriever,
) -> None:
    conn.execute(
        sa.insert(diffs).values(
            crystal_id=crystal_id,
            run_id=run.run_id,
            # Don't know how to fill this. Important?
            # beam_intensity=
            # Don't know how to fill this. Important?
            pinhole=run.info_file.aperture.magnitude,
            # Don't know how to fill this. Important?
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
            data_raw_filename_pattern=run.data_raw_filename_pattern,
            created=path_mtime(Path(run.data_raw_filename_pattern).parent)
            if run.data_raw_filename_pattern
            else datetime.datetime.now(),
            microscope_image_filename_pattern=run.microscope_image_filename_pattern,
        )
    )


def update_diffraction(
    conn: Connection, crystal_id: str, diffs: sa.Table, run: P11Run
) -> None:
    conn.execute(
        sa.update(diffs)
        .values(
            crystal_id=crystal_id,
            run_id=run.run_id,
            # Don't know how to fill this. Important?
            # beam_intensity=
            # Don't know how to fill this. Important?
            pinhole=run.info_file.aperture.magnitude,
            # Don't know how to fill this. Important?
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
            data_raw_filename_pattern=run.data_raw_filename_pattern,
            microscope_image_filename_pattern=run.microscope_image_filename_pattern,
        )
        .where(sa.and_(diffs.c.crystal_id == crystal_id, diffs.c.run_id == run.run_id))
    )


def ingest_diffractions_for_crystals(
    conn: Connection,
    table_diffs: Table,
    table_crystals: Table,
    crystals: List[P11Crystal],
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> List[str]:
    warnings: List[str] = []
    for crystal in crystals:
        this_has_warnings = _ingest_diffractions_for_crystal(
            conn,
            table_diffs,
            table_crystals,
            crystal,
            insert_diffraction_if_not_exists,
            metadata_retriever,
        )
        warnings.extend(this_has_warnings)
    return warnings


def ingest_diffractions_for_targets(
    conn: Connection,
    diffs: Table,
    crystals: Table,
    targets: List[P11Target],
    insert_diffraction_if_not_exists: bool,
    metadata_retriever: MetadataRetriever,
) -> bool:
    has_warnings = False
    for target in targets:
        this_has_warnings = _ingest_diffractions_for_target(
            conn,
            diffs,
            crystals,
            target,
            insert_diffraction_if_not_exists,
            metadata_retriever,
        )
        has_warnings = has_warnings or this_has_warnings
    return has_warnings


def ingest_analysis_result(
    conn: Connection,
    crystal_id: str,
    data_reduction: sa.Table,
    run_id: int,
    analysis_result: AnalysisResult,
) -> int:
    result = conn.execute(
        sa.insert(data_reduction).values(
            crystal_id=crystal_id,
            run_id=run_id,
            analysis_time=analysis_result.analysis_time,
            folder_path=str(analysis_result.base_path),
            mtz_path=str(analysis_result.mtz_file)
            if analysis_result.mtz_file is not None
            else None,
            method=analysis_result.method,
            resolution_cc=analysis_result.resolution_cc,
            resolution_isigma=analysis_result.resolution_isigma,
            a=analysis_result.a,
            b=analysis_result.b,
            c=analysis_result.c,
            alpha=analysis_result.alpha,
            beta=analysis_result.beta,
            gamma=analysis_result.gamma,
            space_group=find_space_group_index_by_name(analysis_result.space_group)
            if isinstance(analysis_result.space_group, str)
            else analysis_result.space_group,
            isigi=analysis_result.isigi,
            rmeas=analysis_result.rmeas,
            cchalf=analysis_result.cchalf,
            rfactor=analysis_result.rfactor,
            Wilson_b=analysis_result.wilson_b,
        )
    )
    return result.inserted_primary_key[0]


def ingest_reductions_for_crystals(
    conn: Connection,
    table_data_reduction: sa.Table,
    table_diffractions: sa.Table,
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

            if (
                conn.execute(
                    sa.select([table_diffractions.c.crystal_id]).where(
                        sa.and_(
                            table_diffractions.c.crystal_id == crystal.crystal_id,
                            table_diffractions.c.run_id == run.run_id,
                        )
                    )
                ).fetchone()
                is None
            ):
                logger.debug(
                    f"crystal {crystal.crystal_id}, run {run.run_id}: cannot ingest data reduction: got no "
                    f"corresponding diffraction image "
                )
                continue

            for process_result in process_results:
                if (
                    conn.execute(
                        sa.select([table_data_reduction.c.data_reduction_id]).where(
                            table_data_reduction.c.folder_path
                            == str(process_result.base_path)
                        )
                    ).fetchone()
                    is not None
                ):
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
                ingest_analysis_result(
                    conn,
                    crystal.crystal_id,
                    table_data_reduction,
                    run.run_id,
                    process_result,
                )
    return warnings
