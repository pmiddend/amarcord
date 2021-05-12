import logging
from typing import List
from typing import Optional

import sqlalchemy as sa
from pint import UnitRegistry
from sqlalchemy import Table

from amarcord.amici.p11.analyze_filesystem import P11Crystal
from amarcord.amici.p11.analyze_filesystem import P11Run
from amarcord.amici.p11.analyze_filesystem import P11Target
from amarcord.amici.p11.db import ReductionMethod
from amarcord.amici.xds.analyze_filesystem import XDSFilesystem
from amarcord.amici.xds.analyze_filesystem import analyze_xds_filesystem
from amarcord.modules.dbcontext import Connection

DETECTOR_NAME = "DECTRIS EIGER 16M"

logger = logging.getLogger(__name__)


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
    dummy_puck: Optional[str],
) -> bool:
    if (
        conn.execute(
            sa.select([diffs.c.crystal_id]).where(
                diffs.c.crystal_id == crystal.crystal_id,
            )
        ).fetchone()
        is None
    ):
        if dummy_puck is not None:
            logger.info(
                "Creating nonexisting crystal %s with dummy puck %s",
                crystal.crystal_id,
                dummy_puck,
            )
            conn.execute(
                sa.insert(crystals).values(
                    crystal_id=crystal.crystal_id,
                    puck_id=dummy_puck,
                    puck_position_id=1,
                )
            )
        else:
            logger.warning(
                "Crystal %s found in file system but not in database",
                crystal.crystal_id,
            )
            return True

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
            and dummy_puck is None
        ):
            logger.info(
                "Diffraction for crystal ID %s, run ID %s does not exist, not adding it",
                crystal.crystal_id,
                run.run_id,
            )
            continue
        logger.info(
            "Crystal ID %s, run %s, updating existing diffraction...",
            crystal.crystal_id,
            run.run_id,
        )
        _ingest_diffraction(conn, crystal.crystal_id, diffs, run)

    return True


def _ingest_diffractions_for_target(
    conn: Connection,
    diffs: sa.Table,
    crystals: sa.Table,
    target: P11Target,
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
            _ingest_diffraction(conn, crystal_id, diffs, run)

    return has_warnings


def _ingest_diffraction(
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
            detector_name=DETECTOR_NAME,
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
    dummy_puck: Optional[str],
) -> bool:
    has_warnings = False
    for crystal in crystals:
        logger.info(
            "Ingesting diffractions for crystal %s (%s run(s))",
            crystal.crystal_id,
            len(crystal.runs),
        )
        this_has_warnings = _ingest_diffractions_for_crystal(
            conn, table_diffs, table_crystals, crystal, dummy_puck
        )
        has_warnings = has_warnings or this_has_warnings
    return has_warnings


def ingest_diffractions_for_targets(
    conn: Connection,
    diffs: Table,
    crystals: Table,
    targets: List[P11Target],
) -> bool:
    has_warnings = False
    for target in targets:
        this_has_warnings = _ingest_diffractions_for_target(
            conn, diffs, crystals, target
        )
        has_warnings = has_warnings or this_has_warnings
    return has_warnings


def ingest_reductions_for_targets(
    conn: Connection,
    crystals: Table,
    data_reduction: sa.Table,
    targets: List[P11Target],
    ureg: UnitRegistry,
) -> bool:
    has_warnings = False
    for target in targets:
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
                if run.processed_path is None:
                    continue

                if (
                    conn.execute(
                        sa.select([data_reduction.c.data_reduction_id]).where(
                            data_reduction.c.folder_path == str(run.processed_path)
                        )
                    ).fetchone()
                    is not None
                ):
                    logger.info(
                        "Data reduction for folder %s already exists",
                        run.processed_path,
                    )
                    continue

                xds_processed = analyze_xds_filesystem(run.processed_path, ureg)
                if isinstance(xds_processed, XDSFilesystem):
                    ingest_xds_result(
                        conn, crystal_id, data_reduction, run, xds_processed
                    )
    return has_warnings


def ingest_xds_result(
    conn: Connection,
    crystal_id: str,
    data_reduction: sa.Table,
    run: P11Run,
    xds_processed: XDSFilesystem,
) -> None:
    conn.execute(
        sa.insert(data_reduction).values(
            crystal_id=crystal_id,
            run_id=run.run_id,
            analysis_time=xds_processed.analysis_time,
            folder_path=str(run.processed_path),
            mtz_path=str(xds_processed.mtz_file)
            if xds_processed.mtz_file is not None
            else None,
            # No idea
            # comment=
            method=ReductionMethod.XDS_FULL,
            resolution_cc=xds_processed.correct_lp.resolution_cc.to(
                "angstrom"
            ).magnitude
            if xds_processed.correct_lp.resolution_cc is not None
            else None,
            resolution_isigma=xds_processed.correct_lp.resolution_isigma.to(
                "angstrom"
            ).magnitude
            if xds_processed.correct_lp.resolution_isigma is not None
            else None,
            a=xds_processed.correct_lp.a.to("angstrom").magnitude,
            b=xds_processed.correct_lp.b.to("angstrom").magnitude,
            c=xds_processed.correct_lp.c.to("angstrom").magnitude,
            alpha=xds_processed.correct_lp.alpha.to("deg").magnitude,
            beta=xds_processed.correct_lp.beta.to("deg").magnitude,
            gamma=xds_processed.correct_lp.gamma.to("deg").magnitude,
            space_group=xds_processed.correct_lp.space_group,
            isigi=xds_processed.correct_lp.isigi,
            rmeas=xds_processed.correct_lp.rmeas,
            cchalf=xds_processed.correct_lp.cchalf,
            rfactor=xds_processed.correct_lp.rfactor,
            Wilson_b=xds_processed.results_file.wilson_b,
        )
    )


def ingest_reductions_for_crystals(
    conn: Connection,
    table_data_reduction: sa.Table,
    crystals: List[P11Crystal],
    ureg: UnitRegistry,
) -> bool:
    has_warnings = False
    for crystal in crystals:
        logger.info("Ingesting reductions for crystal %s", crystal.crystal_id)
        for run in crystal.runs:
            if run.processed_path is None:
                logger.info(
                    "Crystal %s, skipping run %s, no processed_path",
                    crystal.crystal_id,
                    run.run_id,
                )
                continue

            if (
                conn.execute(
                    sa.select([table_data_reduction.c.data_reduction_id]).where(
                        table_data_reduction.c.folder_path == str(run.processed_path)
                    )
                ).fetchone()
                is not None
            ):
                logger.info(
                    "Data reduction for folder %s already exists",
                    run.processed_path,
                )
                continue

            xds_processed = analyze_xds_filesystem(run.processed_path, ureg)
            if isinstance(xds_processed, XDSFilesystem):
                logger.info(
                    "Crystal %s, run %s: ingesting XDS result",
                    crystal.crystal_id,
                    run.run_id,
                )
                ingest_xds_result(
                    conn, crystal.crystal_id, table_data_reduction, run, xds_processed
                )
            else:
                logger.info(
                    "Crystal %s, run %s: xds processing failed, doing nothing",
                    crystal.crystal_id,
                    run.run_id,
                )
    return has_warnings
