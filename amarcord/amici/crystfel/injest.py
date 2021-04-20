import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import cast

from amarcord.amici.crystfel.parser import read_streams
from amarcord.db.db import Connection
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBLinkedIndexingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.modules.json import JSONDict

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.DEBUG
)

logger = logging.getLogger(__name__)

# Select id from IndexingResults where result_file_path matches stream_list,
#  or something 'false' if not found.
# Notes:
#  1. "matches" means that result_file_path contains the same set of filenames
#      as stream_list. There will almost always be more than one element in
#      stream_list, because indexing is almost always split up for speed.
#  2. The order of items in stream_list is not guaranteed (it just comes
#     from globbing for filenames matching a pattern).
#  3. This check is just to prevent duplication in the database, so
#     don't add a new IndexingResults row if nothing is found.
#  4. The filenames in stream_list are absolute paths.
def lookup_indexing_id(
    data_sources: List[DBLinkedDataSource], stream_list: List[Path]
) -> Optional[int]:
    filename_set = set(str(s.resolve()) for s in stream_list)
    return next(
        (
            ir.indexing_result.id
            for ds in data_sources
            for hfr in ds.hit_finding_results
            for ir in hfr.indexing_results
            if ir.indexing_result.result_filename is not None
            and set(ir.indexing_result.result_filename.split(",")) == filename_set
        ),
        None,
    )

    # return find_by(data_sources, lambda ds: lookup_indexing_in_data_source(ds))


# Return id from HitFindingResults where result_filename = input_file_list[0],
#  or something 'false' if not found.
# Notes:
#  1. input_file_list is a list which is currently guaranteed to have exactly one item
#       ... in future, we may need to expand to handle HitFindingResults where
#           the results are split across multiple files, e.g. when using "turbo-cheetah",
#           but this situation may be better handled by using multiple DataSources.
#  2. This check is just to prevent duplication in the database, so
#     don't add a new HitFindingResults row if nothing is found.
#  3. The filename in input_file_list is an absolute path.
def lookup_hitfinding_result(
    data_sources: List[DBLinkedDataSource], input_file_list: List[Path]
) -> Optional[Tuple[DBLinkedDataSource, DBLinkedHitFindingResult]]:
    return next(
        (
            (ds, hfr)
            for ds in data_sources
            for hfr in ds.hit_finding_results
            if set(
                Path(x).resolve()
                for x in hfr.hit_finding_result.result_filename.split(",")
            )
            == set(x.resolve() for x in input_file_list)
        ),
        None,
    )


# Return id from PeakSearchParameters where:
#    software = program
#    software_version = program_version
#    geometry = geometry
#    method = param_json['method']
#    max_num_peaks = param_json['max_num_peaks']
#    adc_threshold = param_json['threshold_adu']
#    minimum_snr = param_json['min_snr']
#    min_pixel_count = param_json['min_pixel_count']
#    max_pixel_count = param_json['max_pixel_count']
#    min_res = param_json['min_res_px']
#    max_res = param_json['max_res_px']
#    bad_pixel_map_filename = NULL
#    bad_pixel_map_hdf5_path = NULL
#    local_bg_radius = param_json['local_bg_radius_px']
#    min_peak_over_neighbour = param_json['min_peak_over_neighbour_adu']
#    min_snr_biggest_pix = param_json['min_snr_of_biggest_pixel']
#    min_snr_peak_pix = param_json['min_snr_of_peak_pixel']
#    min_sig = param_json['min_sig_adu']
#    min_squared_gradient = param_json['min_squared_gradient_adu2']
#  * radius_inner_px = param_json['radius_inner_px']      (small int)
#  * radius_middle_px = param_json['radius_middle_px']    (small int)
#  * radius_outer_px = param_json['radius_outer_px']      (small int)
#  * noise_filter = param_json['noise_filter']            (boolean)
#  * median_filter = param_json['median_filter']          (boolean)
# Notes:
#  1. 'geometry' contains the entire contents of the geometry file in
#     one big string.
#  2. If nothing found in the database, DO add a new PeakSearchParameters
#     and return its id number.
#  3. Items marked '*' above need to be added to the DB schema.  They will
#     probably all be NULL unless peak searching is done with CrystFEL.  Data
#     types are given above.
#  4. When using CrystFEL, the information about bad_pixel_map_* is stored in
#     the geometry file.  It would be messy to try to extract it for the
#     database, because CrystFEL allows multiple bad pixel masks as well as
#     different mask definitions for different detector panels.
#  5. Or, do we want to not put any of these parameters in the database and just
#     save JSON dumps like with indexing?  Most of the parameters are
#     software-specific.  In this case, I would reduce the list to just these:
#       software,software_version,geometry,method,adc_threshold,minimum_snr
def lookup_peaksearch_params(
    p: Dict[str, Any],
    program: str,
    program_version: str,
    geometry: str,
    tag: Optional[str],
) -> DBPeakSearchParameters:
    # FIXME: pmidden: add actual lookup instead of new creation here (too lazy right now)
    return DBPeakSearchParameters(
        id=None,
        method=p["method"],
        software=program,
        tag=tag,
        comment=None,
        software_version=program_version,
        max_num_peaks=p.get("max_num_peaks", None),
        adc_threshold=p.get("adc_threshold", None),
        minimum_snr=p.get("minimum_snr"),
        min_pixel_count=p.get("min_pixel_count"),
        max_pixel_count=p.get("max_pixel_count"),
        min_res=p.get("min_res"),
        max_res=p.get("max_res"),
        bad_pixel_map_filename=None,
        bad_pixel_map_hdf5_path=None,
        local_bg_radius=p.get("local_bg_radius"),
        min_peak_over_neighbor=p.get("min_peak_over_neighbor"),
        min_snr_biggest_pix=p.get("min_snr_biggest_pix"),
        min_snr_peak_pix=p.get("min_snr_peak_pix"),
        min_sig=p.get("min_sig"),
        min_squared_gradient=p.get("min_squared_gradient"),
        geometry=geometry,
    )


# Return HitFindingParameters.id where:
#   min_peaks = param_json['min_num_peaks']
#   software = program
#   software_version = program_version
# Notes:
#  1. DO add a new HitFindingParameters if nothing is found.
#  2. When adding a new entry, git_repository and git_SHA = NULL.
def lookup_hitfinding_params(
    param_json: Dict[str, Any], program: str, program_version: str, tag: Optional[str]
) -> DBHitFindingParameters:
    logger.info(
        "Looking up HitFindingParameters for {}/{}/{}".format(
            program, program_version, param_json["min_num_peaks"]
        )
    )
    # FIXME: Search and use existing one here
    return DBHitFindingParameters(
        id=None,
        min_peaks=param_json["min_num_peaks"],
        tag=tag,
        comment=None,
        software=program,
        software_version=program_version,
    )


# Return DataSource.id where source matches input_files
# Notes:
#  1. DO add a new DataSource if nothing is found
#  2. The definition of 'matches' above is complicated, as discussed.
#     It requires examining each input file, finding the "real" files,
#     Then looking for those in the database.
#  3. For the purposes of this script, we can probably assume that the whole
#     run is processed, i.e. train_id is just NULL.
#  4. In our usage, the input files should always be EuXFEL VDS files,
#     because we have only two cases (running via Cheetah, running on VDS).
#  5. input_files is a list which is currently guaranteed to have one item
#      (see note #1 for lookup_hitfinding_results)
#  6. If the input doesn't look like a VDS file, this procedure should
#      return None.  This will help protect against creating spurious
#      DataSources when filenames get mis-recognised.
def lookup_datasource_id(
    data_sources: List[DBLinkedDataSource],
    number_of_frames: int,
    run_id: int,
    input_files: List[Path],
) -> Optional[DBLinkedDataSource]:
    # FIXME: this needs the test if the input "looks like a VDS file"
    logger.info("Looking up DataSource for {}".format(input_files))

    def datasource_matches(ds: DBDataSource) -> bool:
        if ds.source is None:
            return False
        files = ds.source.get("files", None)
        if files is None:
            return False
        return files == input_files and ds.run_id == run_id

    matching_sources = [x for x in data_sources if datasource_matches(x.data_source)]
    if matching_sources:
        return matching_sources[0]
    return DBLinkedDataSource(
        data_source=DBDataSource(
            id=None,
            run_id=run_id,
            number_of_frames=number_of_frames,
            source={"files": [str(s.resolve()) for s in input_files]},
            tag=None,
            comment=None,
        ),
        hit_finding_results=[],
    )


# Return IndexingParameters.id where:
#   software = program
#   software_version = program_version
#   parameters = param_json
#   methods = param_json['methods']
#   geometry = geometry
# Notes:
#  1. DO add a new IndexingParameters if nothing is found.
#  2. git_repository, git_SHA and command_line = NULL
#  3. Might add a command-line option for tag and comment.  Set them to
#     NULL until then.
def lookup_indexing_params(
    param_json: JSONDict,
    program: str,
    program_version: str,
    geometry: str,
    tag: Optional[str],
) -> Optional[DBIndexingParameters]:
    # FIXME: Add lookup instead of plain add
    return DBIndexingParameters(
        id=None,
        tag=tag,
        comment=None,
        software=program,
        software_version=program_version,
        command_line="",
        parameters=param_json,
        methods=cast(list, param_json["methods"]),
        geometry=geometry,
    )


# Return IntegrationParameters.id where:
#  method = param_json['method']
#  center_boxes = (param_json['method'] contains '-cen')
#  overpredict = param_json['overpredict']
#  push_res = param_json['push_res_invm']
#  radius_inner = param_json['radius_inner_px']
#  radius_middle = param_json['radius_middle_px']
#  radius_outer = param_json['radius_outer_px']
# Notes:
#  1. DO add a new IntegrationParameters if nothing is found.
#  2. git_repository, git_SHA should perhaps be added to DB (NULL here)
#  3. Might add a command-line option for tag and comment.  Set them to
#     NULL until then.
def lookup_integration_params(
    param_json: JSONDict, program: str, program_version: str, tag: Optional[str]
) -> Optional[DBIntegrationParameters]:
    # FIXME: add insert if not exists
    return DBIntegrationParameters(
        id=None,
        tag=tag,
        comment=None,
        software=program,
        software_version=program_version,
        method=param_json.get("method"),  # type: ignore
        center_boxes=param_json.get("center_boxes"),  # type: ignore
        overpredict=param_json.get("overpredict"),  # type: ignore
        push_res=param_json.get("push_res"),  # type: ignore
        radius_inner=param_json.get("radius_inner"),  # type: ignore
        radius_middle=param_json.get("radius_middle"),  # type: ignore
        radius_outer=param_json.get("radius_outer"),  # type: ignore
    )


# Create a new HitFindingResults, and return 'id', where:
#   peak_search_parameters = peaksearch_params
#   hit_finding_parameters = hitfinding_params
#   data_source_id = data_source_id
#   timestamp = timestamp
#   tag = NULL
#   comment = NULL
#   number_hits = num_hits
#   hit_rate = hit_rate
#   average_peaks_event = average_peaks_event
#   average_resolution = average_resolution
#   result_filename = stream_list
#   result_type = stream_file
# Notes:
#  1. May add a command-line option for tag/comment later
#  2. stream_list is a list of absolute filenames
#  3. I've checked that there is no matching HitFindingResults already
#      (though, this check may be unreliable, as we've discussed
#      - let's see how it goes)
#  4. 'timestamp' is as returned from os.path.getmtime()
def insert_hitfinding_result(
    peaksearch_params: DBPeakSearchParameters,
    hitfinding_params: DBHitFindingParameters,
    data_source: DBLinkedDataSource,
    timestamp: Optional[float],
    num_hits: int,
    hit_rate: float,
    average_peaks_event: float,
    average_resolution: float,
    stream_list: List[Path],
    tag: Optional[str],
) -> DBLinkedHitFindingResult:
    logger.info("Inserting HitFindingResult:")
    logger.info("              Data source: {}".format(data_source.data_source.id))
    logger.info("   Peak search parameters: {}".format(peaksearch_params))
    logger.info("   Hit finding parameters: {}".format(hitfinding_params))
    logger.info("                Stream(s): {}".format(stream_list))
    logger.info("  {} frames, hit rate {}".format(num_hits, hit_rate))
    logger.info("  Average {} peaks/frame".format(average_peaks_event))
    logger.info("  Average resolution {} m^-1".format(average_resolution))
    logger.info("                Timestamp: {}".format(timestamp))
    logger.info("                Tag: {}".format(tag))

    return DBLinkedHitFindingResult(
        DBHitFindingResult(
            id=None,
            peak_search_parameters_id=peaksearch_params.id,  # type: ignore
            hit_finding_parameters_id=hitfinding_params.id,  # type: ignore
            data_source_id=data_source.data_source.id,
            # FIXME: This should be an array in SQL
            result_filename=",".join(str(s) for s in stream_list),
            peaks_filename=None,
            result_type="stream",
            average_peaks_event=average_peaks_event,
            average_resolution=average_resolution,
            number_of_hits=num_hits,
            hit_rate=hit_rate,
            tag=tag,
            comment=None,
        ),
        peaksearch_params,
        hitfinding_params,
        indexing_results=[],
    )


# Create a new IndexingResults, and return 'id', where:
#  hitfinding_results_id = hitfinding_results
#  peaksearch_parameter_id = peaksearch_params
#  indexing_parameter_id = indexing_params
#  integration_parameter_id = integration_params
#  ambiguity_parameter_id = NULL
#  indexing_results_id = NULL
#  timestamp = timestamp
#  tag = NULL
#  comment = NULL
#  result_file_path = stream_list
#  num_indexed = num_indexed
#  num_crystals = num_crystals
# Notes:
#  1. Perhaps we should rename peaks_id to hitfinding_results_id?
#  2. We have already checked that there is no matching IndexingResults already
#  3. stream_list is a list with almost certainly more than one element.
#      (needs to be turned into "filename,filename,filename")
#  4. Command-line option for tag/comment possibly to be added in future.
#  5. 'timestamp' is as returned from os.path.getmtime()
def insert_indexing_results(
    peaksearch_params: DBPeakSearchParameters,
    indexing_params: DBIndexingParameters,
    integration_params: DBIntegrationParameters,
    stream_list: List[Path],
    num_indexed: int,
    num_crystals: int,
    timestamp: Optional[float],
    tag: Optional[str],
) -> DBLinkedIndexingResult:
    logger.info("Mock insert of IndexingResults:")
    logger.info("   Peak search parameters: {}".format(peaksearch_params))
    logger.info("      Indexing parameters: {}".format(indexing_params))
    logger.info("   Integration parameters: {}".format(integration_params))
    logger.info("                Stream(s): {}".format(stream_list))
    logger.info("  {} indexed frames, {} crystals".format(num_indexed, num_crystals))
    logger.info("                Timestamp: {}".format(timestamp))
    return DBLinkedIndexingResult(
        DBIndexingResult(
            id=None,
            # IDs are left out here - we're ingesting that "lazily" later
            hit_finding_result_id=None,  # type: ignore
            peak_search_parameters_id=None,  # type: ignore
            indexing_parameters_id=None,  # type: ignore
            integration_parameters_id=None,  # type: ignore
            num_indexed=num_indexed,
            num_crystals=num_crystals,
            tag=tag,
            comment=None,
            result_filename=",".join(str(s) for s in stream_list),
        ),
        peak_search_parameters=peaksearch_params,
        indexing_parameters=indexing_params,
        integration_parameters=integration_params,
    )


def read_json(fn: Path) -> Dict[str, Any]:
    with fn.open("r") as f:
        return json.load(f)


def harvest_folder(
    data_sources: List[DBLinkedDataSource],
    folder: Path,
    run_id: int,
    stream_patt: str,
    harvest_fn: Path,
    tag: Optional[str],
) -> List[DBLinkedDataSource]:
    logger.debug("Harvesting {} ({}, {})".format(folder, stream_patt, harvest_fn))

    stream_list = list(folder.glob(stream_patt))

    # No streams found? Fine, return nothing
    if not stream_list:
        logger.info("No streams found with glob %s in folder %s", stream_patt, folder)
        return []

    if lookup_indexing_id(data_sources, stream_list) is not None:
        logger.debug(
            "Stopping because {} is already in IndexingResults.".format(stream_list)
        )
        return []

    stream_stuff = read_streams(stream_list)

    # See note #1 for lookup_hitfinding_results
    # This check we planned at first but it doesn't work with multi turbo Cheetah(tm)
    # if len(stream_stuff.input_files) != 1:
    #     logger.warning(
    #         "Stopping because there is more than one filename in the stream: %s",
    #         stream_stuff.input_files,
    #     )
    #     return []

    # Read harvest file
    harvest_json = read_json(harvest_fn)
    if not harvest_json:
        logger.error("Couldn't read harvest file {}".format(harvest_fn))
        return []

    data_source_and_hitfinding_result = lookup_hitfinding_result(
        data_sources, stream_stuff.input_files
    )

    # If using hdf5/cxi peaks, link to already-existing hitfinding ID.
    # Otherwise, find the parameters matching ours, creating if necessary.
    if harvest_json["peaksearch"]["method"] in ("hdf5", "cxi"):
        if data_source_and_hitfinding_result is None:
            logger.warning(
                "Indexing was performed using peaks from hit finding, "
                "but I couldn't find the HitFindingResults entry."
            )
            return []
        logger.info("Using peak search parameters from existing result")
        peaksearch_params = data_source_and_hitfinding_result[1].peak_search_parameters
    else:
        logger.info("Adding new peak search parameters")
        peaksearch_params = lookup_peaksearch_params(
            harvest_json["peaksearch"],
            "CrystFEL,indexamajig",
            stream_stuff.version if stream_stuff.version is not None else "1.0",
            stream_stuff.geometry,
            tag,
        )

    if data_source_and_hitfinding_result is None:
        logger.info("HitFindingResult not found - adding my own")

        data_source = lookup_datasource_id(
            data_sources, stream_stuff.n_frames, run_id, stream_stuff.input_files
        )
        if data_source is None:
            logger.info("Stopping because DataSource couldn't be created.")
            return []

        hitfinding_params = lookup_hitfinding_params(
            harvest_json["hitfinding"],
            "CrystFEL,indexamajig",
            stream_stuff.version if stream_stuff.version is not None else "1.0",
            tag,
        )

        hitfinding_result = insert_hitfinding_result(
            peaksearch_params,
            hitfinding_params,
            data_source,
            stream_stuff.timestamp,
            stream_stuff.num_hits,
            stream_stuff.hit_rate,
            stream_stuff.average_peaks_event,
            stream_stuff.average_resolution,
            stream_list,
            tag,
        )

        data_source.hit_finding_results.append(hitfinding_result)

        data_source_and_hitfinding_result = data_source, hitfinding_result
    else:
        logger.info(
            "Found existing HitFindingResult, psp ID: ",
            data_source_and_hitfinding_result[1].peak_search_parameters.id,
        )

    indexing_params = lookup_indexing_params(
        harvest_json["indexing"],
        "CrystFEL,indexamajig",
        stream_stuff.version if stream_stuff.version is not None else "1.0",
        stream_stuff.geometry,
        tag,
    )

    integration_params = lookup_integration_params(
        harvest_json["integration"],
        "CrystFEL,indexamajig",
        stream_stuff.version if stream_stuff.version is not None else "1.0",
        tag,
    )

    # Create IndexingResults in database
    indexing_result = insert_indexing_results(
        peaksearch_params,
        indexing_params,  # type: ignore
        integration_params,  # type: ignore
        stream_list,
        stream_stuff.num_indexed,
        stream_stuff.num_crystals,
        stream_stuff.timestamp,
        tag,
    )

    data_source_and_hitfinding_result[1].indexing_results.append(indexing_result)

    return [data_source_and_hitfinding_result[0]]


def ingest_data_source(
    db: DB,
    conn: Connection,
    force_create_run: bool,
    proposal_id: ProposalId,
    run_ids: Set[int],
    ds: DBLinkedDataSource,
) -> Set[int]:
    result_run_ids = run_ids.copy()
    if ds.data_source.run_id not in run_ids:
        if not force_create_run:
            raise ValueError(
                f"Couldn't injest data source, run {ds.data_source.run_id} is not known"
            )
        db.add_run(
            conn,
            proposal_id,
            ds.data_source.run_id,
            sample_id=None,
            attributi=RawAttributiMap({}),
        )
        result_run_ids.add(ds.data_source.run_id)

    data_source_id: int
    if ds.data_source.id is None:
        data_source_id = db.add_data_source(conn, ds.data_source)
    else:
        data_source_id = ds.data_source.id

    for hfr in ds.hit_finding_results:
        hfr_id = ingest_hit_finding_result(db, conn, data_source_id, hfr)

        for ir in hfr.indexing_results:
            ingest_indexing_result(db, conn, data_source_id, hfr_id, ir)

    return result_run_ids


def ingest_indexing_result(
    db: DB,
    conn: Connection,
    data_source_id: int,
    hfr_id: int,
    ir: DBLinkedIndexingResult,
) -> int:
    if ir.peak_search_parameters.id is None and ir.indexing_result.id is not None:
        raise ValueError(
            f"invalid injest: for data source {data_source_id} and hit finding result {hfr_id}, we have a known "
            f"indexing result {ir.indexing_result.id} but unknown peak search parameters "
        )
    if ir.integration_parameters.id is None and ir.indexing_result.id is not None:
        raise ValueError(
            f"invalid injest: for data source {data_source_id} and hit finding result {hfr_id}, we have a known "
            f"indexing result {ir.indexing_result.id} but unknown integration parameters "
        )
    if ir.indexing_parameters.id is None and ir.indexing_result.id is not None:
        raise ValueError(
            f"invalid injest: for data source {data_source_id} and hit finding result {hfr_id}, we have a known "
            f"indexing result {ir.indexing_result.id} but unknown indexing_parameters "
        )
    psp_id: int
    if ir.peak_search_parameters.id is None:
        psp_id = db.add_peak_search_parameters(conn, ir.peak_search_parameters)
    else:
        psp_id = ir.peak_search_parameters.id
    ip_id: int
    if ir.integration_parameters.id is None:
        ip_id = db.add_integration_parameters(conn, ir.integration_parameters)
    else:
        ip_id = ir.integration_parameters.id
    indp_id: int
    if ir.indexing_parameters.id is None:
        indp_id = db.add_indexing_parameters(conn, ir.indexing_parameters)
    else:
        indp_id = ir.indexing_parameters.id
    if ir.indexing_result.id is None:
        result = replace(
            ir.indexing_result,
            hit_finding_result_id=hfr_id,
            indexing_parameters_id=indp_id,
            peak_search_parameters_id=psp_id,
            integration_parameters_id=ip_id,
        )
        return db.add_indexing_result(conn, result)
    return ir.indexing_result.id


def ingest_hit_finding_result(
    db: DB, conn: Connection, data_source_id: int, hfr: DBLinkedHitFindingResult
) -> int:
    if hfr.peak_search_parameters.id is None and hfr.hit_finding_result.id is not None:
        raise ValueError(
            f"invalid injest: for data source {data_source_id}, we have a known hit finding result "
            f"{hfr.hit_finding_result.id} but unknown peak search parameters "
        )
    if hfr.hit_finding_parameters.id is None and hfr.hit_finding_result.id is not None:
        raise ValueError(
            f"invalid injest: for data source {data_source_id}, we have a known hit finding result "
            f"{hfr.hit_finding_result.id} but unknown hit finding parameters "
        )
    psp_id: int
    if hfr.peak_search_parameters.id is None:
        psp_id = db.add_peak_search_parameters(conn, hfr.peak_search_parameters)
    else:
        psp_id = hfr.peak_search_parameters.id
    hfp_id: int
    if hfr.hit_finding_parameters.id is None:
        hfp_id = db.add_hit_finding_parameters(conn, hfr.hit_finding_parameters)
    else:
        hfp_id = hfr.hit_finding_parameters.id
    if hfr.hit_finding_result.id is None:
        result = replace(
            hfr.hit_finding_result,
            data_source_id=data_source_id,
            peak_search_parameters_id=psp_id,
            hit_finding_parameters_id=hfp_id,
        )
        return db.add_hit_finding_result(conn, result)
    return hfr.hit_finding_result.id
