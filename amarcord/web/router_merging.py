import datetime
import re
from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy import true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.attributi import utc_int_to_utc_datetime
from amarcord.db.constants import SPACE_GROUP_ATTRIBUTO
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.merge_result import JsonMergeJobFinishedInput
from amarcord.db.merge_result import JsonMergeJobStartedInput
from amarcord.db.merge_result import JsonMergeJobStartedOutput
from amarcord.db.orm_utils import determine_point_group_from_indexing_results
from amarcord.db.orm_utils import determine_run_indexing_metadata
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import orm_encode_json_merge_parameters_to_json
from amarcord.web.fastapi_utils import retrieve_runs_matching_data_set
from amarcord.web.fastapi_utils import safe_create_new_event
from amarcord.web.json_models import JsonMergeJob
from amarcord.web.json_models import JsonMergeJobFinishOutput
from amarcord.web.json_models import JsonQueueMergeJobInput
from amarcord.web.json_models import JsonQueueMergeJobOutput
from amarcord.web.json_models import JsonReadMergeResultsOutput
from amarcord.web.router_files import encode_file_output
from amarcord.web.router_indexing import json_indexing_job_from_orm

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


@router.post(
    "/api/merging/{mergeResultId}/start",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def merge_job_started(
    mergeResultId: int,  # noqa: N803
    json_result: JsonMergeJobStartedInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonMergeJobStartedOutput:
    job_logger = logger.bind(merge_result_id=mergeResultId)

    async with session.begin():
        merge_result = (
            await session.scalars(
                select(orm.MergeResult).where(orm.MergeResult.id == mergeResultId),
            )
        ).one()
        merge_result.job_id = json_result.job_id
        merge_result.started = utc_int_to_utc_datetime(json_result.time)
        merge_result.job_status = DBJobStatus.RUNNING
        job_logger.info(f"merge result now has job id {json_result.job_id}, is running")
        await session.commit()
    return JsonMergeJobStartedOutput(
        time=utc_datetime_to_utc_int(datetime.datetime.now(datetime.timezone.utc)),
        time_local=utc_datetime_to_local_int(
            datetime.datetime.now(datetime.timezone.utc)
        ),
    )


@router.post(
    "/api/merging/{mergeResultId}/finish",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def merge_job_finished(
    mergeResultId: int,  # noqa: N803
    json_result: JsonMergeJobFinishedInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonMergeJobFinishOutput:
    job_logger = logger.bind(merge_result_id=mergeResultId)

    job_logger.info("merge job has finished")

    async with session.begin():
        current_merge_result_status = (
            await session.scalars(
                select(orm.MergeResult)
                .where(orm.MergeResult.id == mergeResultId)
                .options(
                    selectinload(orm.MergeResult.indexing_results).selectinload(
                        orm.IndexingResult.run,
                    ),
                )
                .options(selectinload(orm.MergeResult.refinement_results)),
            )
        ).one_or_none()

        if current_merge_result_status is None:
            job_logger.error("merge job not found in DB")
            return JsonMergeJobFinishOutput(result=False)

        if current_merge_result_status.stopped is not None:
            job_logger.warning(
                "merge result has a stopped date already; this might be fine though",
            )

        stopped_time = datetime.datetime.now(datetime.timezone.utc)

        beamtime_id = current_merge_result_status.indexing_results[0].run.beamtime_id

        # In a weird turn of events, this merge result might have
        # never "started" and gone to finished directly. In that case,
        # start and finish time are the same by convention.
        if current_merge_result_status.started is None:
            current_merge_result_status.started = stopped_time
        current_merge_result_status.stopped = stopped_time
        # Update the log if we have been given one, otherwise let it
        # stay the same. This is important for the case where the
        # actual job sends us the real log, but the daemon, at the
        # same time, recognizes the cancelled job.
        if json_result.latest_log is not None:
            current_merge_result_status.recent_log = json_result.latest_log
        if json_result.error is not None:
            await safe_create_new_event(
                job_logger,
                session,
                beamtime_id,
                f"merge result {mergeResultId} finished with error `{json_result.error}`",
                EventLogLevel.INFO,
                "API",
            )
            job_logger.error(f"semantic error in json content: {json_result.error}")
            current_merge_result_status.job_error = json_result.error
            current_merge_result_status.job_status = DBJobStatus.DONE
            return JsonMergeJobFinishOutput(result=False)

        assert (
            json_result.result is not None
        ), f"both error and result are none in output: {json_result}"

        await safe_create_new_event(
            job_logger,
            session,
            beamtime_id,
            f"merge result {mergeResultId} finished successfully",
            EventLogLevel.INFO,
            "API",
        )
        current_merge_result_status.stopped = stopped_time
        current_merge_result_status.job_status = DBJobStatus.DONE

        r = json_result.result
        cmrs = current_merge_result_status
        if r.ambigator_fg_graph_file_id is not None:
            cmrs.ambigator_fg_graph_file_id = r.ambigator_fg_graph_file_id
        cmrs.mtz_file_id = r.mtz_file_id
        cmrs.fom_snr = r.fom.snr
        cmrs.fom_wilson = r.fom.wilson
        cmrs.fom_ln_k = r.fom.wilson
        cmrs.fom_discarded_reflections = r.fom.discarded_reflections
        cmrs.fom_one_over_d_from = r.fom.one_over_d_from
        cmrs.fom_one_over_d_to = r.fom.one_over_d_to
        cmrs.fom_redundancy = r.fom.redundancy
        cmrs.fom_completeness = r.fom.completeness
        cmrs.fom_measurements_total = r.fom.measurements_total
        cmrs.fom_reflections_total = r.fom.reflections_total
        cmrs.fom_reflections_possible = r.fom.reflections_possible
        cmrs.fom_r_split = r.fom.r_split
        cmrs.fom_r1i = r.fom.r1i
        cmrs.fom_2 = r.fom.r2
        cmrs.fom_cc = r.fom.cc
        cmrs.fom_ccstar = r.fom.ccstar
        cmrs.fom_ccano = r.fom.ccano
        cmrs.fom_crdano = r.fom.crdano
        cmrs.fom_rano = r.fom.rano
        cmrs.fom_rano_over_r_split = r.fom.rano_over_r_split
        cmrs.fom_d1sig = r.fom.d1sig
        cmrs.fom_d2sig = r.fom.d2sig
        cmrs.fom_outer_resolution = r.fom.outer_shell.resolution
        cmrs.fom_outer_ccstar = r.fom.outer_shell.ccstar
        cmrs.fom_outer_r_split = r.fom.outer_shell.r_split
        cmrs.fom_outer_cc = r.fom.outer_shell.cc
        cmrs.fom_outer_unique_reflections = r.fom.outer_shell.unique_reflections
        cmrs.fom_outer_completeness = r.fom.outer_shell.completeness
        cmrs.fom_outer_redundancy = r.fom.outer_shell.redundancy
        cmrs.fom_outer_snr = r.fom.outer_shell.snr
        cmrs.fom_outer_min_res = r.fom.outer_shell.min_res
        cmrs.fom_outer_max_res = r.fom.outer_shell.max_res
        for shell in r.detailed_foms:
            cmrs.shell_foms.append(
                orm.MergeResultShellFom(
                    one_over_d_centre=shell.one_over_d_centre,
                    nref=shell.nref,
                    d_over_a=shell.d_over_a,
                    min_res=shell.min_res,
                    max_res=shell.max_res,
                    cc=shell.cc,
                    ccstar=shell.ccstar,
                    r_split=shell.r_split,
                    reflections_possible=shell.reflections_possible,
                    completeness=shell.completeness,
                    measurements=shell.measurements,
                    redundancy=shell.redundancy,
                    snr=shell.snr,
                    mean_i=shell.mean_i,
                ),
            )

        for rr in json_result.result.refinement_results:
            cmrs.refinement_results.append(
                orm.RefinementResult(
                    merge_result_id=current_merge_result_status.id,
                    pdb_file_id=rr.pdb_file_id,
                    mtz_file_id=rr.mtz_file_id,
                    r_free=rr.r_free,
                    r_work=rr.r_work,
                    rms_bond_angle=rr.rms_bond_angle,
                    rms_bond_length=rr.rms_bond_length,
                ),
            )
        return JsonMergeJobFinishOutput(result=True)


async def determine_space_group_from_indexing_results(
    session: AsyncSession,
    beamtime_id: int,
    indexing_results_matching_params: list[orm.IndexingResult],
) -> None | str:
    # get all chemicals in all runs related to the indexing results (attributo ID is not even important)
    chemical_ids_in_runs = select(orm.RunHasAttributoValue.chemical_value).where(
        (
            orm.RunHasAttributoValue.run_id.in_(
                ir.run_id for ir in indexing_results_matching_params
            )
        )
        & (orm.RunHasAttributoValue.chemical_value.is_not(None)),
    )
    # attributi, plural, but there should be only one since names are hopefully unique
    space_group_chemical_attributi = (
        select(orm.Attributo.id)
        .where(
            (orm.Attributo.name == SPACE_GROUP_ATTRIBUTO)
            & (orm.Attributo.beamtime_id == beamtime_id),
        )
        .scalar_subquery()
    )
    select_all_space_groups = select(orm.ChemicalHasAttributoValue.string_value).where(
        (orm.ChemicalHasAttributoValue.attributo_id == space_group_chemical_attributi)
        & (orm.ChemicalHasAttributoValue.chemical_id.in_(chemical_ids_in_runs)),
    )
    space_groups = set(
        s.strip()
        for s in (await session.scalars(select_all_space_groups.distinct()))
        if s is not None and s.strip()
    )

    if len(space_groups) > 1:
        raise HTTPException(
            status_code=400,
            detail="Found more than one space group! The runs I chose have (internal) IDs "
            + ", ".join(str(ir.run_id) for ir in indexing_results_matching_params)
            + ", which results in the following space groups (determined by going through all chemicals in the runs): "
            + ", ".join(space_groups)
            + ". To correct this, you have to either specify a separate space group while merging, or (better choice, probably) take care of the space groups for your chemicals: you should have exactly one space group for all chemicals for all runs.",
        )
    if not space_groups:
        return None
    return next(iter(space_groups))


CRYSTFEL_POINT_GROUPS = [
    "1",
    "-1",
    "2/m",
    "2",
    "m",
    "mmm",
    "222",
    "mm2",
    "4/m",
    "4",
    "-4",
    "4/mmm",
    "422",
    "-42m",
    "-4m2",
    "4mm",
    "3_R",
    "-3_R",
    "32_R",
    "3m_R",
    "-3m_R",
    "3_H",
    "-3_H",
    "321_H",
    "312_H",
    "3m1_H",
    "31m_H",
    "-3m1_H",
    "-31m_H",
    "6/m",
    "6",
    "-6",
    "6/mmm",
    "622",
    "-62m",
    "-6m2",
    "6mm",
    "23",
    "m-3",
    "432",
    "-43m",
    "m-3m",
]

CRYSTFEL_SYMMETRY_RE = re.compile(r"-?[hkl],-?[hkl],-?[hkl](;-?[hkl],-?[hkl],-?[hkl])*")


def validate_ambigator_command_line(s: str) -> None:
    if s.strip() == "":
        return

    args = s.split(" ")

    current_index = 0
    while current_index < len(args):
        thisarg = args[current_index]
        if thisarg == "-w":
            if current_index == len(args) - 1:
                raise ValueError(
                    "nothing after -w, should be followed by a point group"
                )
            pg = args[current_index + 1]
            if pg not in CRYSTFEL_POINT_GROUPS:
                raise ValueError(f"after -w: {pg} not a valid point group")
            current_index += 2
            continue

        current_index += 1

        if thisarg == "--really-random":
            continue

        equal_split = thisarg.split("=", maxsplit=2)
        if len(equal_split) != 2:
            raise ValueError(
                f"argument {thisarg} not valid (should contain an equal sign)"
            )

        key_with_dashes, value = equal_split

        if not key_with_dashes.startswith("--"):
            raise ValueError(f"unknown argument {key_with_dashes}")

        key = key_with_dashes[2:]

        if key == "symmetry":
            if value not in CRYSTFEL_POINT_GROUPS:
                raise ValueError(f"after --symmetry: {value} not a valid point group")
        elif key == "operator":
            if not CRYSTFEL_SYMMETRY_RE.fullmatch(value):
                raise ValueError(
                    f"after --operator: {value} not a valid symmetry operator"
                )
        elif key == "iterations":
            # stupid, but that's enough (not a good error message maybe, but that's for the malicious user)
            int(value)
        elif key in ("highres", "lowres"):
            float(value)
        elif key == "ncorr":
            int(value)
        else:
            raise ValueError(f"unknown argument: {thisarg}")


@router.post(
    "/api/merging",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def queue_merge_job(
    input_: JsonQueueMergeJobInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonQueueMergeJobOutput:
    logger.info("start creating merge result")
    async with session.begin():
        # First, we have to collect all indexing results that match
        # the given indexing parameter ID and which also match the
        # data set.
        #
        # For that, unfortunately, we have to collect all the runs
        # that match the data set first, which is the most
        # time-consuming step.
        merge_params = input_.merge_parameters
        try:
            validate_ambigator_command_line(
                input_.merge_parameters.ambigator_command_line
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"ambigator issue: {e.args[0]}")

        data_set = (
            await session.scalars(
                select(orm.DataSet).where(orm.DataSet.id == input_.data_set_id),
            )
        ).one_or_none()
        if data_set is None:
            raise HTTPException(
                status_code=400,
                detail=f'Data set with ID "{input_.data_set_id}" not found',
            )
        beamtime_id = (await data_set.awaitable_attrs.experiment_type).beamtime_id
        runs_matching_ds = await retrieve_runs_matching_data_set(
            session,
            input_.data_set_id,
            beamtime_id,
        )
        if not runs_matching_ds:
            raise HTTPException(
                status_code=400,
                detail=f"Data set with ID {input_.data_set_id} has no runs!",
            )
        indexing_parameters = (
            await session.scalars(
                select(orm.IndexingParameters).where(
                    orm.IndexingParameters.id == input_.indexing_parameters_id,
                ),
            )
        ).one_or_none()
        if indexing_parameters is None:
            raise HTTPException(
                status_code=400,
                detail=f"Indexing parameters with ID {input_.indexing_parameters_id} not found!",
            )
        indexing_results_matching_params: list[orm.IndexingResult] = [
            ir
            for ir in await session.scalars(
                select(orm.IndexingResult)
                .where(orm.IndexingResult.run_id.in_(r.id for r in runs_matching_ds))
                .options(selectinload(orm.IndexingResult.indexing_parameters)),
            )
            if orm.are_indexing_parameters_equal(
                ir.indexing_parameters,
                indexing_parameters,
            )
            and ir.job_status == DBJobStatus.DONE
            and ir.job_error is None
        ]
        if not indexing_results_matching_params:
            raise HTTPException(
                status_code=400,
                detail=f"No (finished) indexing results found for parameters ID {input_.indexing_parameters_id}!",
            )
        # The point group we either get from the user as an input, or
        # from the chemicals attached to the runs, which are, in turn,
        # attached to the indexing results.
        if input_.merge_parameters.point_group:
            point_group = input_.merge_parameters.point_group
        else:
            try:
                point_group = await determine_point_group_from_indexing_results(
                    session,
                    beamtime_id,
                    indexing_results_matching_params,
                )
            except ValueError as e:
                raise HTTPException(status_code=400, detail=e.args[0])
        # The space group we either get from the user as an input, or
        # from the chemicals attached to the runs, which are, in turn,
        # attached to the indexing results.
        if input_.merge_parameters.space_group:
            space_group = input_.merge_parameters.space_group
        else:
            space_group = await determine_space_group_from_indexing_results(
                session,
                beamtime_id,
                indexing_results_matching_params,
            )
        # The cell description is easier to get than the point group,
        # since it's already in the indexing parameters, and all of
        # those parameters have to be the same amongst the indexing
        # results, so we can just take the one we get as an input.
        #
        # However, the cell description is technically optional while
        # indexing (you might want to find out what cell it is!) but
        # for compare_hkl, you need it again. So we fail here, late,
        # when we have none.
        cell_description = (
            input_.merge_parameters.cell_description
            if input_.merge_parameters.cell_description
            else indexing_parameters.cell_description
        )

        if not cell_description:
            raise HTTPException(
                status_code=400,
                detail="Have no cell description in the indexing results, cannot start merging! I have chosen indexing results "
                + ", ".join(str(ir.id) for ir in indexing_results_matching_params),
            )

        negative_handling = merge_params.negative_handling
        polarisation = merge_params.polarisation
        logger.info(
            "all checks passed, creating new merge result with indexing results "
            + ", ".join(str(ir.id) for ir in indexing_results_matching_params),
        )
        new_merge_result = orm.MergeResult(
            created=datetime.datetime.now(datetime.timezone.utc),
            cell_description=cell_description,
            recent_log="",
            negative_handling=negative_handling,
            job_status=DBJobStatus.QUEUED,
            started=None,
            stopped=None,
            point_group=point_group,
            space_group=space_group,
            job_id=None,
            job_error=None,
            mtz_file_id=None,
            ambigator_fg_graph_file_id=None,
            ambigator_command_line=merge_params.ambigator_command_line.strip(),
            input_merge_model=orm.MergeModel(merge_params.merge_model),
            input_scale_intensities=ScaleIntensities(merge_params.scale_intensities),
            input_post_refinement=merge_params.post_refinement,
            input_iterations=merge_params.iterations,
            input_polarisation_angle=(
                polarisation.angle if polarisation is not None else None
            ),
            input_polarisation_percent=(
                polarisation.percent if polarisation is not None else None
            ),
            input_start_after=merge_params.start_after,
            input_stop_after=merge_params.stop_after,
            input_rel_b=merge_params.rel_b,
            input_no_pr=merge_params.no_pr,
            input_force_bandwidth=merge_params.force_bandwidth,
            input_force_radius=merge_params.force_radius,
            input_force_lambda=merge_params.force_lambda,
            input_no_delta_cc_half=merge_params.no_delta_cc_half,
            input_max_adu=merge_params.max_adu,
            input_min_measurements=merge_params.min_measurements,
            input_logs=merge_params.logs,
            input_min_res=merge_params.min_res,
            input_push_res=merge_params.push_res,
            input_w=merge_params.w,
        )
        new_merge_result.indexing_results.extend(indexing_results_matching_params)
        session.add(new_merge_result)
        await session.flush()
        await safe_create_new_event(
            logger,
            session,
            beamtime_id,
            f"merge result {new_merge_result.id} started",
            EventLogLevel.INFO,
            "API",
        )
        return JsonQueueMergeJobOutput(merge_result_id=new_merge_result.id)


async def _read_files_from_indexing_in_merge_result(
    session: AsyncSession,
    mr: orm.MergeResult,
) -> list[orm.File]:
    result: list[orm.File] = []
    for indexing_result in mr.indexing_results:
        run = indexing_result.run

        indexing_metadata = await determine_run_indexing_metadata(session, run)

        if isinstance(indexing_metadata, str):
            raise Exception(
                f"couldn't get indexing metadata for merge result {mr.id}, run {run.id} (external ID {run.external_id}): {indexing_metadata}",
            )

        result.extend(await indexing_metadata.chemical.awaitable_attrs.files)
    return result


@router.get(
    "/api/merging",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def read_merge_jobs(
    session: Annotated[AsyncSession, Depends(get_orm_db)],
    status: None | DBJobStatus = None,
) -> JsonReadMergeResultsOutput:
    async def encode_single_merge_job(mr: orm.MergeResult) -> JsonMergeJob:
        assert mr.indexing_results
        return JsonMergeJob(
            id=mr.id,
            job_id=mr.job_id,
            job_status=mr.job_status,
            point_group=mr.point_group,
            cell_description=mr.cell_description,
            parameters=orm_encode_json_merge_parameters_to_json(mr),
            indexing_results=[
                # File paths are not important in this request, we only need it for starting the indexing
                await json_indexing_job_from_orm(ir, with_files=False)
                for ir in mr.indexing_results
            ],
            # One of the rare instances where we're fetching something deliberately lazy, at least for now:
            # retrieve the files for all indexing results, getting the chemical first, then the files.
            # Let's see if performance holds up.
            files_from_indexing=[
                encode_file_output(file)
                for file in await _read_files_from_indexing_in_merge_result(session, mr)
            ],
        )

    return JsonReadMergeResultsOutput(
        merge_jobs=[
            await encode_single_merge_job(mr)
            for mr in await session.scalars(
                select(orm.MergeResult)
                .options(
                    selectinload(orm.MergeResult.indexing_results)
                    .selectinload(orm.IndexingResult.run)
                    .selectinload(orm.Run.beamtime),
                )
                .options(
                    selectinload(orm.MergeResult.indexing_results).selectinload(
                        orm.IndexingResult.indexing_parameters,
                    ),
                )
                .options(
                    selectinload(orm.MergeResult.indexing_results)
                    .selectinload(orm.IndexingResult.run)
                    .selectinload(orm.Run.attributo_values)
                    .selectinload(orm.RunHasAttributoValue.attributo),
                )
                .where(
                    orm.MergeResult.job_status == status
                    if status is not None
                    else true(),
                ),
            )
        ],
    )


@router.get(
    "/api/merging/{mergeResultId}/log",
    tags=["processing"],
    response_model_exclude_defaults=True,
    response_class=PlainTextResponse,
)
async def merge_job_get_log(
    mergeResultId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> str:
    async with session.begin():
        return (
            (
                await session.scalars(
                    select(orm.MergeResult).where(
                        orm.MergeResult.id == mergeResultId,
                    ),
                )
            ).one()
        ).recent_log
