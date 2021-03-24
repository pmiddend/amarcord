import logging
import os
import re
import sys
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import Generator

from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.tables import create_tables
from amarcord.modules.cheetah import cheetah_read_crawler_config_file
from amarcord.modules.cheetah import cheetah_read_crawler_runs_table
from amarcord.modules.cheetah import cheetah_read_recipe
from amarcord.modules.dbcontext import DBContext

AMARCORD_DB_ENV_VAR = "AMARCORD_DB"

logger = logging.getLogger(__name__)


def cheetah_to_database(config_file: Path) -> Generator[DBDataSource, None, None]:
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

        ds = DBDataSource(
            id=None,
            run_id=line.run_id,
            number_of_frames=line.nprocessed,
            hit_finding_results=[],
            source={"files": source_files},
            tag=None,
            comment=None,
        )

        recipe = cheetah_read_recipe(line_hdf_dir / line.recipe)

        peak_search_params = DBPeakSearchParameters(
            id=None,
            software="Cheetah",
            command_line="unclear",
            tag=line.dataset,
            comment=None,
            method=f"hitfinderAlgorithm={recipe.algorithm}",
            software_version="unclear",
            software_git_repository="https://github.com/antonbarty/cheetah",
            # I just took the latest one at time of writing
            software_git_sha="5c174325c03ec884b918384157953b3521694036",
            max_num_peaks=recipe.npeaks_max,
            adc_threshold=recipe.adc,
            minimum_snr=recipe.min_snr,
            min_pixel_count=recipe.min_pix_count,
            max_pixel_count=recipe.max_pix_count,
            min_res=recipe.min_res,
            max_res=recipe.max_res,
            bad_pixel_filename=str(recipe.bad_pixel_map),
            local_bg_radius=recipe.local_bg_radius,
            min_peak_over_neighbor=None,
            min_snr_biggest_pix=None,
            min_snr_peak_pix=None,
            min_sig=None,
            min_squared_gradient=None,
            geometry_filename=str(recipe.geometry_file),
        )

        hit_finding_parameters = DBHitFindingParameters(
            id=None,
            min_peaks=recipe.npeaks,
            tag=None,
            comment=None,
        )

        if line.nhits is None or line.hit_rate is None:
            logger.debug(f"Either {line.nhits} or {line.hit_rate} isn't set, skipping")
            continue

        hit_finding_result = DBHitFindingResult(
            id=None,
            data_source_id=None,
            peak_search_parameters=peak_search_params,
            hit_finding_parameters=hit_finding_parameters,
            result_filename=str(peaks_results_file),
            number_of_hits=line.nhits,
            hit_rate=line.hit_rate,
            indexing_results=[],
            tag=None,
            comment=None,
        )

        ds.hit_finding_results.append(hit_finding_result)
        yield ds


def deep_compare_data_source(left: DBDataSource, right: DBDataSource) -> bool:
    if left != right:
        return False
    if len(left.hit_finding_results) != len(right.hit_finding_results):
        return False
    for left_hfr, right_hfr in zip(left.hit_finding_results, right.hit_finding_results):
        if left_hfr != right_hfr:
            return False
        if left_hfr.indexing_results != right_hfr.indexing_results:
            return False
    return True


def deep_ingest_data_source(ds: DBDataSource, db: DB, conn: Connection) -> None:
    with conn.begin():
        ds_id = db.add_data_source(conn, ds)
        for hfr in ds.hit_finding_results:
            hfr_id = db.add_hit_finding_result(conn, replace(hfr, data_source_id=ds_id))

            for ir in hfr.indexing_results:
                db.add_indexing_result(conn, replace(ir, hit_finding_results_id=hfr_id))


@dataclass(frozen=True)
class CheetahIngestResults:
    number_of_ingested_data_sources: int


def ingest_cheetah_internal(
    config_file: Path, db: DB, conn: Connection
) -> CheetahIngestResults:
    """
    Ingest cheetah "testable" with custom DB object and connection. Not to be used from a script, since there,
    we want something simpler.
    """
    number_of_ingested_data_sources = 0
    existing_indexings = db.retrieve_analysis_data_sources(conn)
    for ds in cheetah_to_database(config_file):
        if not any(
            deep_compare_data_source(ds, existing_ds)
            for existing_ds in existing_indexings
        ):
            deep_ingest_data_source(ds, db, conn)
            number_of_ingested_data_sources += 1
    return CheetahIngestResults(
        number_of_ingested_data_sources=number_of_ingested_data_sources
    )


def ingest_cheetah(config_file: Path) -> CheetahIngestResults:
    env_var = os.environ.get(AMARCORD_DB_ENV_VAR, None)
    if env_var is None:
        sys.stderr.write(
            f'Didn\'t find the environment variable "{AMARCORD_DB_ENV_VAR}".\n'
            f"Please specify it so we have a connection to AMARCORD.\n"
            f"The format is:\n\n"
            f"mysql+pymysql://$username:$password@$host/$dbname?proposal_id=$proposal_id\n\n"
            f"Important variables are:\n"
            f"$username => the user name for the database connection\n"
            f"$password => the password for the database connection\n"
            f"$host => the host for the database connection\n"
            f"$dbname => name of the database to connect to\n"
            f"$proposal_id => proposal ID\n"
        )
        sys.exit(1)

    proposal_match = re.search(r"\?proposal_id=(\d+)$", env_var)
    if not proposal_match:
        sys.stderr.write(
            f'The environment variable "{AMARCORD_DB_ENV_VAR}" doesn\'t have a proposal ID in it.\n'
            f"The format for the DB connection URL is:\n\n"
            f"mysql+pymysql://$username:$password@$host/$dbname?proposal_id=$proposal_id\n"
        )
        sys.exit(1)

    url_match = env_var[0 : proposal_match.start()]

    dbcontext = DBContext(url_match)

    tables = create_tables(dbcontext)
    db = DB(dbcontext, tables)
    with db.connect() as conn:
        return ingest_cheetah_internal(config_file, db, conn)
