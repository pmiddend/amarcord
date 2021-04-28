import logging
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import Generator
from typing import List
from typing import Tuple
from typing import cast

from amarcord.amici.cheetah.parser import cheetah_read_crawler_config_file
from amarcord.amici.cheetah.parser import cheetah_read_crawler_runs_table
from amarcord.amici.cheetah.parser import cheetah_read_recipe
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.util import find_by

SOFTWARE_VERSION = None

CHEETAH = "Cheetah"

AMARCORD_DB_ENV_VAR = "AMARCORD_DB"

logger = logging.getLogger(__name__)


def cheetah_to_database(config_file: Path) -> Generator[DBLinkedDataSource, None, None]:
    """
    Convert the cheetah file(s) to database objects. This nicely splits the process of ingesting Cheetah results in
    two distinct steps: conversion and actual ingestion.
    """

    crawler_config = cheetah_read_crawler_config_file(config_file)
    crawler_csv = crawler_config.runs_table_location

    if not crawler_csv.exists():
        raise ValueError(
            f"didn't find run table, looked inside {crawler_config.runs_table_location}"
        )

    hdf5_base_dir = (config_file.parent / crawler_config.hdf5dir).resolve()
    for line in cheetah_read_crawler_runs_table(crawler_csv):
        if line.hdf5_directory is None or line.recipe is None:
            logger.debug("Line has no HDF5 directory or no ini file, skipping")
            continue

        if line.cheetah_status == "Not finished":
            logger.debug("Line is not finished yet")
            continue

        # noinspection SpellCheckingInspection
        line_hdf_dir = hdf5_base_dir / line.hdf5_directory
        agipdfiles = line_hdf_dir / "agipdfiles.txt"
        peaks_results_file = line_hdf_dir / "peaks.txt"

        if not agipdfiles.exists() or not peaks_results_file.exists():
            logger.debug(
                f"Either {agipdfiles} or {peaks_results_file} doesn't exist, skipping"
            )
            continue

        with agipdfiles.open("r") as f:
            source_files = [source_line.strip() for source_line in f.readlines()]

        if line.nprocessed is None:
            logger.debug("nprocessed isn't set, skipping")
            continue

        ds = DBLinkedDataSource(
            DBDataSource(
                id=None,
                run_id=line.run_id,
                number_of_frames=line.nprocessed,
                source={"files": source_files},
                tag=None,
                comment=None,
            ),
            hit_finding_results=[],
        )

        recipe_path = line_hdf_dir / line.recipe

        if not recipe_path.exists():
            logger.debug(f"Recipe path {recipe_path} doesn't exist, skipping")
            continue

        recipe = cheetah_read_recipe(recipe_path)

        peak_search_params = DBPeakSearchParameters(
            id=None,
            software=CHEETAH,
            software_version=SOFTWARE_VERSION,
            tag=line.dataset,
            comment=None,
            method=f"hitfinderAlgorithm={recipe.algorithm}",
            max_num_peaks=recipe.npeaks_max,
            adc_threshold=recipe.adc,
            minimum_snr=recipe.min_snr,
            min_pixel_count=recipe.min_pix_count,
            max_pixel_count=recipe.max_pix_count,
            min_res=recipe.min_res,
            max_res=recipe.max_res,
            bad_pixel_map_filename=str(recipe.bad_pixel_map),
            local_bg_radius=recipe.local_bg_radius,
            min_peak_over_neighbor=None,
            min_snr_biggest_pix=None,
            min_snr_peak_pix=None,
            min_sig=None,
            min_squared_gradient=None,
            geometry=None
            # geometry_filename=str(recipe.geometry_file),
        )

        hit_finding_parameters = DBHitFindingParameters(
            id=None,
            min_peaks=recipe.npeaks,
            tag=None,
            comment=None,
            software=CHEETAH,
            software_version=SOFTWARE_VERSION,
        )

        if line.nhits is None or line.hit_rate is None:
            logger.debug(f"Either {line.nhits} or {line.hit_rate} isn't set, skipping")
            continue

        hit_finding_result = DBLinkedHitFindingResult(
            hit_finding_result=DBHitFindingResult(
                id=None,
                peak_search_parameters_id=None,  # type: ignore
                hit_finding_parameters_id=None,  # type: ignore
                data_source_id=None,
                result_filename=",".join(str(s) for s in line_hdf_dir.glob("*cxi")),
                peaks_filename=str(peaks_results_file),
                number_of_hits=line.nhits,
                hit_rate=line.hit_rate,
                tag=None,
                comment=None,
                # This cannot be 0 but I don't know where to get it from in Cheetah
                average_resolution=0,
                # This cannot be 0 but I don't know where to get it from in Cheetah
                average_peaks_event=0,
                result_type="application/x-hdf",
            ),
            peak_search_parameters=peak_search_params,
            hit_finding_parameters=hit_finding_parameters,
            indexing_results=[],
        )

        ds.hit_finding_results.append(hit_finding_result)
        yield ds


def deep_ingest_data_source(ds: DBLinkedDataSource, db: DB, conn: Connection) -> int:
    with conn.begin():
        ds_id = db.add_data_source(conn, ds.data_source)
        for hfr in ds.hit_finding_results:
            psp_id = db.add_peak_search_parameters(conn, hfr.peak_search_parameters)
            hfp_id = db.add_hit_finding_parameters(conn, hfr.hit_finding_parameters)
            hfr_id = db.add_hit_finding_result(
                conn,
                replace(
                    hfr.hit_finding_result,
                    data_source_id=ds_id,
                    peak_search_parameters_id=psp_id,
                    hit_finding_parameters_id=hfp_id,
                ),
            )

            for ir in hfr.indexing_results:
                ip_id = db.add_indexing_parameters(conn, ir.indexing_parameters)
                intp_id = db.add_integration_parameters(conn, ir.integration_parameters)
                db.add_indexing_result(
                    conn,
                    replace(
                        ir.indexing_result,
                        hit_finding_result_id=hfr_id,
                        peak_search_parameters_id=psp_id,
                        indexing_parameters_id=ip_id,
                        integration_parameters_id=intp_id,
                    ),
                )
        return ds_id


@dataclass(frozen=True)
class CheetahIngestResults:
    new_data_source_and_run_ids: List[Tuple[int, int]]


def ingest_cheetah(
    config_file: Path,
    db: DB,
    conn: Connection,
    proposal_id: ProposalId,
    force_run_creation: bool,
) -> CheetahIngestResults:
    """
    Ingest cheetah "testable" with custom DB object and connection. Not to be used from a script, since there,
    we want something simpler.
    """
    existing_indexings = db.retrieve_analysis_data_sources(conn)
    run_ids = set(db.retrieve_run_ids(conn, proposal_id))
    result: List[Tuple[int, int]] = []
    for ds in cheetah_to_database(config_file):
        run_id = ds.data_source.run_id
        if run_id not in run_ids:
            if not force_run_creation:
                continue
            logger.info(f"Creating run {run_id}")
            db.add_run(conn, proposal_id, run_id, None, RawAttributiMap({}))
            run_ids.add(run_id)

        existing_ds = find_by(
            # pylint: disable=cell-var-from-loop
            existing_indexings,
            lambda eds: eds.data_source == ds.data_source,
        )

        # We haven't found the matching data source? So ingest everything
        if existing_ds is None:
            ds_id = deep_ingest_data_source(ds, db, conn)
            result.append((ds_id, ds.data_source.run_id))  # type: ignore
        else:
            # We have found a matching data source, now ingest all hit finding results
            for hfr in ds.hit_finding_results:
                existing_hfr = find_by(
                    existing_ds.hit_finding_results,
                    # pylint: disable=cell-var-from-loop
                    lambda ehfr: hfr.hit_finding_result == ehfr.hit_finding_result
                    and hfr.hit_finding_parameters == ehfr.hit_finding_parameters
                    and hfr.peak_search_parameters == ehfr.peak_search_parameters,
                )

                if existing_hfr is None:
                    with conn.begin():
                        # We haven't found the hit finding result, so ingest it
                        psp_id = db.add_peak_search_parameters(
                            conn, hfr.peak_search_parameters
                        )
                        hfp_id = db.add_hit_finding_parameters(
                            conn, hfr.hit_finding_parameters
                        )
                        _hfr_id = db.add_hit_finding_result(
                            conn,
                            replace(
                                hfr.hit_finding_result,
                                peak_search_parameters_id=psp_id,
                                hit_finding_parameters_id=hfp_id,
                                data_source_id=existing_ds.data_source.id,
                            ),
                        )
                        result.append(
                            (
                                cast(int, existing_ds.data_source.id),
                                existing_ds.data_source.run_id,
                            )
                        )
    return CheetahIngestResults(new_data_source_and_run_ids=result)
