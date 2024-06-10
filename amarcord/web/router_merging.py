import datetime
from typing import Callable

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy import true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db import orm
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.merge_result import JsonMergeJobFinishedInput
from amarcord.db.merge_result import JsonMergeJobStartedInput
from amarcord.db.merge_result import JsonMergeJobStartedOutput
from amarcord.db.merge_result import JsonMergeResultFom
from amarcord.db.merge_result import JsonMergeResultInternal
from amarcord.db.merge_result import JsonMergeResultOuterShell
from amarcord.db.merge_result import JsonMergeResultShell
from amarcord.db.merge_result import JsonRefinementResultInternal
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.util import group_by
from amarcord.web.fastapi_utils import format_run_id_intervals
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import safe_create_new_event
from amarcord.web.json_models import JsonMergeJob
from amarcord.web.json_models import JsonMergeJobFinishOutput
from amarcord.web.json_models import JsonMergeParameters
from amarcord.web.json_models import JsonMergeResult
from amarcord.web.json_models import JsonMergeResultStateDone
from amarcord.web.json_models import JsonMergeResultStateError
from amarcord.web.json_models import JsonMergeResultStateQueued
from amarcord.web.json_models import JsonMergeResultStateRunning
from amarcord.web.json_models import JsonPolarisation
from amarcord.web.json_models import JsonQueueMergeJobForDataSetInput
from amarcord.web.json_models import JsonQueueMergeJobForDataSetOutput
from amarcord.web.json_models import JsonReadMergeResultsOutput
from amarcord.web.json_models import JsonRefinementResult
from amarcord.web.router_files import encode_file_output
from amarcord.web.router_indexing import json_indexing_job_from_orm

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


def json_merge_parameters_from_orm(mr: orm.MergeResult) -> JsonMergeParameters:
    return JsonMergeParameters(
        point_group=mr.point_group,
        negative_handling=mr.negative_handling,
        merge_model=mr.input_merge_model,
        scale_intensities=mr.input_scale_intensities,
        post_refinement=mr.input_post_refinement,
        iterations=mr.input_iterations,
        polarisation=(
            JsonPolarisation(
                angle=mr.input_polarisation_angle,
                percent=mr.input_polarisation_percent,
            )
            if mr.input_polarisation_angle is not None
            and mr.input_polarisation_percent is not None
            else None
        ),
        start_after=mr.input_start_after,
        stop_after=mr.input_stop_after,
        rel_b=mr.input_rel_b,
        no_pr=mr.input_no_pr if mr.input_no_pr is not None else False,
        force_bandwidth=mr.input_force_bandwidth,
        force_radius=mr.input_force_radius,
        force_lambda=mr.input_force_lambda,
        no_delta_cc_half=mr.input_no_delta_cc_half,
        max_adu=mr.input_max_adu,
        min_measurements=mr.input_min_measurements,
        logs=mr.input_logs,
        min_res=mr.input_min_res,
        push_res=mr.input_push_res,
        w=mr.input_w,
    )


def encode_merge_result(
    mr: orm.MergeResult,
    run_id_formatter: None | Callable[[RunInternalId], int] = None,
) -> JsonMergeResult:
    result = JsonMergeResult(
        id=mr.id,
        created=datetime_to_attributo_int(mr.created),
        # We don't export the indexing results here yet. No clear reason other than laziness
        runs=format_run_id_intervals(
            (run_id_formatter(ir.run_id) if run_id_formatter is not None else ir.run_id)
            for ir in mr.indexing_results
        ),
        state_queued=(
            JsonMergeResultStateQueued(queued=True)
            if mr.job_status == DBJobStatus.QUEUED
            else None
        ),
        state_running=(
            JsonMergeResultStateRunning(
                started=datetime_to_attributo_int(mr.started),
                job_id=mr.job_id,
                latest_log=mr.recent_log,
            )
            if mr.started is not None and mr.job_id is not None and mr.stopped is None
            else None
        ),
        state_error=(
            JsonMergeResultStateError(
                started=datetime_to_attributo_int(mr.started),
                stopped=datetime_to_attributo_int(mr.stopped),
                error=mr.job_error,
                latest_log=mr.recent_log,
            )
            if mr.started is not None
            and mr.stopped is not None
            and mr.job_error is not None
            else None
        ),
        state_done=(
            JsonMergeResultStateDone(
                started=datetime_to_attributo_int(mr.started),
                stopped=datetime_to_attributo_int(mr.stopped),
                result=JsonMergeResultInternal(
                    detailed_foms=[
                        JsonMergeResultShell(
                            one_over_d_centre=s.one_over_d_centre,
                            nref=s.nref,
                            d_over_a=s.d_over_a,
                            min_res=s.min_res,
                            max_res=s.max_res,
                            cc=s.cc,
                            ccstar=s.ccstar,
                            r_split=s.r_split,
                            reflections_possible=s.reflections_possible,
                            completeness=s.completeness,
                            measurements=s.measurements,
                            redundancy=s.redundancy,
                            snr=s.snr,
                            mean_i=s.mean_i,
                        )
                        for s in mr.shell_foms
                    ],
                    refinement_results=[
                        JsonRefinementResultInternal(
                            id=rr.id,
                            pdb_file_id=rr.pdb_file_id,
                            mtz_file_id=rr.mtz_file_id,
                            r_free=rr.r_free,
                            r_work=rr.r_work,
                            rms_bond_angle=rr.rms_bond_angle,
                            rms_bond_length=rr.rms_bond_length,
                        )
                        for rr in mr.refinement_results
                    ],
                    mtz_file_id=mr.mtz_file_id,
                    fom=JsonMergeResultFom(
                        snr=mr.fom_snr,  # type: ignore
                        wilson=mr.fom_wilson,
                        ln_k=mr.fom_ln_k,
                        discarded_reflections=mr.fom_discarded_reflections,  # type: ignore
                        one_over_d_from=mr.fom_one_over_d_from,  # type: ignore
                        one_over_d_to=mr.fom_one_over_d_to,  # type: ignore
                        redundancy=mr.fom_redundancy,  # type: ignore
                        completeness=mr.fom_completeness,  # type: ignore
                        measurements_total=mr.fom_measurements_total,  # type: ignore
                        reflections_total=mr.fom_reflections_total,  # type: ignore
                        reflections_possible=mr.fom_reflections_possible,  # type: ignore
                        r_split=mr.fom_r_split,  # type: ignore
                        r1i=mr.fom_r1i,  # type: ignore
                        r2=mr.fom_2,  # type: ignore
                        cc=mr.fom_cc,  # type: ignore
                        ccstar=mr.fom_ccstar,  # type: ignore
                        ccano=mr.fom_ccano,
                        crdano=mr.fom_crdano,
                        rano=mr.fom_rano,
                        rano_over_r_split=mr.fom_rano_over_r_split,
                        d1sig=mr.fom_d1sig,  # type: ignore
                        d2sig=mr.fom_d2sig,  # type: ignore
                        outer_shell=JsonMergeResultOuterShell(
                            resolution=mr.fom_outer_resolution,  # type: ignore
                            ccstar=mr.fom_outer_ccstar,  # type: ignore
                            r_split=mr.fom_outer_r_split,  # type: ignore
                            cc=mr.fom_outer_cc,  # type: ignore
                            unique_reflections=mr.fom_outer_unique_reflections,  # type: ignore
                            completeness=mr.fom_outer_completeness,  # type: ignore
                            redundancy=mr.fom_outer_redundancy,  # type: ignore
                            snr=mr.fom_outer_snr,  # type: ignore
                            min_res=mr.fom_outer_min_res,  # type: ignore
                            max_res=mr.fom_outer_max_res,  # type: ignore
                        ),
                    ),
                ),
            )
            if mr.started is not None
            and mr.stopped is not None
            and mr.fom_snr is not None
            else None
        ),
        parameters=json_merge_parameters_from_orm(mr),
        refinement_results=[
            JsonRefinementResult(
                id=rr.id,
                merge_result_id=rr.merge_result_id,
                pdb_file_id=rr.pdb_file_id,
                mtz_file_id=rr.mtz_file_id,
                r_free=rr.r_free,
                r_work=rr.r_work,
                rms_bond_angle=rr.rms_bond_angle,
                rms_bond_length=rr.rms_bond_length,
            )
            for rr in mr.refinement_results
        ],
    )
    return result


@router.post(
    "/api/merging/start/{mergeResultId}",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def merge_job_started(
    mergeResultId: int,
    json_merge_result: JsonMergeJobStartedInput,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonMergeJobStartedOutput:
    job_logger = logger.bind(merge_result_id=mergeResultId)

    async with session.begin():
        merge_result = (
            await session.scalars(
                select(orm.MergeResult).where(orm.MergeResult.id == mergeResultId)
            )
        ).one()
        merge_result.job_id = json_merge_result.job_id
        merge_result.started = datetime_from_attributo_int(json_merge_result.time)
        merge_result.job_status = DBJobStatus.RUNNING
        job_logger.info(
            f"merge result now has job id {json_merge_result.job_id}, is running"
        )
        await session.commit()
    return JsonMergeJobStartedOutput(
        time=datetime_to_attributo_int(datetime.datetime.now(datetime.timezone.utc))
    )


@router.post(
    "/api/merging/finish/{mergeResultId}",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def merge_job_finished(
    mergeResultId: int,
    json_merge_result: JsonMergeJobFinishedInput,
    session: AsyncSession = Depends(get_orm_db),
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
                        orm.IndexingResult.run
                    )
                )
                .options(selectinload(orm.MergeResult.refinement_results))
            )
        ).one_or_none()

        if current_merge_result_status is None:
            job_logger.error("merge job not found in DB")
            return JsonMergeJobFinishOutput(result=False)

        if current_merge_result_status.stopped is not None:
            job_logger.warning(
                "merge result has a stopped date already; this might be fine though"
            )
            recent_log = ""
        else:
            recent_log = current_merge_result_status.recent_log

        stopped_time = datetime.datetime.now(datetime.timezone.utc)

        beamtime_id = current_merge_result_status.indexing_results[0].run.beamtime_id

        # In a weird turn of events, this merge result might have
        # never "started" and gone to finished directly. In that case,
        # start and finish time are the same by convention.
        if current_merge_result_status.started is None:
            current_merge_result_status.started = stopped_time
        current_merge_result_status.stopped = stopped_time
        current_merge_result_status.recent_log = recent_log
        if json_merge_result.error is not None:
            await safe_create_new_event(
                job_logger,
                session,
                beamtime_id,
                f"merge result {mergeResultId} finished with error `{json_merge_result.error}`",
                EventLogLevel.INFO,
                "API",
            )
            job_logger.error(
                f"semantic error in json content: {json_merge_result.error}"
            )
            current_merge_result_status.job_error = json_merge_result.error
            current_merge_result_status.job_status = DBJobStatus.DONE
            return JsonMergeJobFinishOutput(result=False)

        assert (
            json_merge_result.result is not None
        ), f"both error and result are none in output: {json_merge_result}"

        await safe_create_new_event(
            job_logger,
            session,
            beamtime_id,
            f"merge result {mergeResultId} finished successfully",
            EventLogLevel.INFO,
            "API",
        )
        current_merge_result_status.stopped = stopped_time
        current_merge_result_status.recent_log = recent_log
        current_merge_result_status.job_status = DBJobStatus.DONE

        r = json_merge_result.result
        cmrs = current_merge_result_status
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
                )
            )

        for rr in json_merge_result.result.refinement_results:
            cmrs.refinement_results.append(
                orm.RefinementResult(
                    pdb_file_id=rr.pdb_file_id,
                    mtz_file_id=rr.mtz_file_id,
                    r_free=rr.r_free,
                    r_work=rr.r_work,
                    rms_bond_angle=rr.rms_bond_angle,
                    rms_bond_length=rr.rms_bond_length,
                )
            )
        await session.flush()
        return JsonMergeJobFinishOutput(result=True)


@router.post(
    "/api/merging/queue/{dataSetId}",
    tags=["merging"],
    response_model_exclude_defaults=True,
)
async def queue_merge_job_for_data_set(
    dataSetId: int,
    input_: JsonQueueMergeJobForDataSetInput,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonQueueMergeJobForDataSetOutput:
    logger.info("start creating merge result for data set")
    async with session.begin():
        strict_mode = input_.strict_mode
        beamtime_id = input_.beamtime_id
        params = input_.merge_parameters
        attributi = list(
            (
                await session.scalars(
                    select(orm.Attributo).where(
                        orm.Attributo.beamtime_id == beamtime_id
                    )
                )
            ).all()
        )
        data_set = (
            await session.scalars(
                select(orm.DataSet).where(orm.DataSet.id == dataSetId)
            )
        ).one_or_none()
        if data_set is None:
            raise HTTPException(
                status_code=400, detail=f'Data set with ID "{dataSetId}" not found'
            )
        all_runs = (
            await session.scalars(
                select(orm.Run).where(orm.Run.beamtime_id == beamtime_id)
            )
        ).all()
        attributo_types: dict[AttributoId, AttributoType] = {
            AttributoId(a.id): schema_dict_to_attributo_type(a.json_schema)
            for a in attributi
        }
        run_attributi_maps: dict[
            int, dict[AttributoId, None | orm.RunHasAttributoValue]
        ] = {r.id: {ra.attributo_id: ra for ra in r.attributo_values} for r in all_runs}
        data_set_attributi_map = {
            dsa.attributo_id: dsa for dsa in data_set.attributo_values
        }
        runs = [
            r
            for r in all_runs
            if r.experiment_type_id == data_set.experiment_type_id
            and run_matches_dataset(
                attributo_types, run_attributi_maps[r.id], data_set_attributi_map
            )
        ]
        if not runs:
            raise HTTPException(
                status_code=400, detail=f"Data set with ID {dataSetId} has no runs!"
            )
        indexing_results_by_run_id: dict[int, list[orm.IndexingResult]] = group_by(
            (
                ir
                for ir in (
                    await session.scalars(
                        select(orm.IndexingResult, orm.Run)
                        .join(orm.IndexingResult.run)
                        .where(orm.Run.beamtime_id == beamtime_id)
                    )
                )
                if ir.job_status == DBJobStatus.DONE and ir.job_error is None
            ),
            lambda ir: ir.run_id,
        )
        chosen_indexing_results: list[orm.IndexingResult] = []
        cell_descriptions: set[CrystFELCellFile] = set()
        point_groups: set[str] = set()
        for run in runs:
            irs = sorted(
                indexing_results_by_run_id.get(run.id, []),
                key=lambda ir: ir.id,
                reverse=True,
            )
            if not irs:
                if strict_mode:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Run {run.id} has no indexing results and strict mode is on; cannot merge",
                    )
                continue
            chosen_result = irs[0]
            if chosen_result.cell_description:
                parsed_description = parse_cell_description(
                    chosen_result.cell_description
                )
                if parsed_description is None:
                    logger.warning(
                        f"indexing result {irs[0].id} has an invalid cell description: {chosen_result.cell_description}, ignoring this result"
                    )
                else:
                    cell_descriptions.add(parsed_description)
            if chosen_result.point_group:
                point_groups.add(chosen_result.point_group)
            chosen_indexing_results.append(chosen_result)
        if not chosen_indexing_results:
            detail = "Found no indexing results for the runs " + ",".join(
                str(r.id) for r in runs
            )
            logger.error(detail)
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        # Shouldn't happen, since for each indexing result we
        # automatically have a cell description and point group, but
        # the type system is too clunky to express that easily.
        if not cell_descriptions:
            detail = "Found no cell descriptions for the runs " + ",".join(
                str(r.id) for r in runs
            )
            logger.error(detail)
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        if not point_groups:
            detail = "Found no cell descriptions for the runs " + ",".join(
                str(r.id) for r in runs
            )
            logger.error(detail)
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        if len(cell_descriptions) > 1:
            detail = (
                "We have more than one cell description and cannot merge: "
                + ", ".join(coparse_cell_description(c) for c in cell_descriptions)
            )
            logger.error(detail)
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        if len(point_groups) > 1:
            detail = "We have more than one point group and cannot merge: " + ", ".join(
                f for f in point_groups
            )
            logger.error(detail)
            raise HTTPException(
                status_code=400,
                detail=detail,
            )
        negative_handling = params.negative_handling
        polarisation = params.polarisation
        logger.info(
            "all checks passed, creating new merge result with indexing results "
            + " ,".join(str(ir.id) for ir in chosen_indexing_results)
        )
        new_merge_result = orm.MergeResult(
            created=datetime.datetime.now(datetime.timezone.utc),
            recent_log="",
            negative_handling=negative_handling,
            job_status=DBJobStatus.QUEUED,
            started=None,
            stopped=None,
            point_group=next(iter(point_groups)),
            cell_description=coparse_cell_description(next(iter(cell_descriptions))),
            job_id=None,
            job_error=None,
            mtz_file_id=None,
            input_merge_model=orm.MergeModel(params.merge_model),
            input_scale_intensities=ScaleIntensities(params.scale_intensities),
            input_post_refinement=params.post_refinement,
            input_iterations=params.iterations,
            input_polarisation_angle=(
                polarisation.angle if polarisation is not None else None
            ),
            input_polarisation_percent=(
                polarisation.percent if polarisation is not None else None
            ),
            input_start_after=params.start_after,
            input_stop_after=params.stop_after,
            input_rel_b=params.rel_b,
            input_no_pr=params.no_pr,
            input_force_bandwidth=params.force_bandwidth,
            input_force_radius=params.force_radius,
            input_force_lambda=params.force_lambda,
            input_no_delta_cc_half=params.no_delta_cc_half,
            input_max_adu=params.max_adu,
            input_min_measurements=params.min_measurements,
            input_logs=params.logs,
            input_min_res=params.min_res,
            input_push_res=params.push_res,
            input_w=params.w,
        )
        new_merge_result.indexing_results.extend(chosen_indexing_results)
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
        return JsonQueueMergeJobForDataSetOutput(merge_result_id=new_merge_result.id)


@router.get(
    "/api/merging",
    tags=["analysis", "processing"],
    response_model_exclude_defaults=True,
)
async def read_merge_jobs(
    status: None | DBJobStatus,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadMergeResultsOutput:
    async def encode_single_merge_job(mr: orm.MergeResult) -> JsonMergeJob:
        assert mr.indexing_results
        return JsonMergeJob(
            id=mr.id,
            job_id=mr.job_id,
            job_status=mr.job_status,
            cell_description=mr.cell_description,
            point_group=mr.point_group,
            parameters=json_merge_parameters_from_orm(mr),
            indexing_results=[
                json_indexing_job_from_orm(ir) for ir in mr.indexing_results
            ],
            # One of the rare instances where we're fetching something deliberately lazy, at least for now:
            # retrieve the files for all indexing results, getting the chemical first, then the files.
            # Let's see if performance holds up.
            files_from_indexing=[
                encode_file_output(file)
                for ir in mr.indexing_results
                for file in await (
                    await ir.awaitable_attrs.chemical
                ).awaitable_attrs.files
            ],
        )

    result = JsonReadMergeResultsOutput(
        merge_jobs=[
            await encode_single_merge_job(ij)
            for ij in await session.scalars(
                select(orm.MergeResult)
                .options(
                    selectinload(orm.MergeResult.indexing_results)
                    .selectinload(orm.IndexingResult.run)
                    .selectinload(orm.Run.beamtime)
                )
                .where(
                    orm.MergeResult.job_status == status
                    if status is not None
                    else true()
                )
            )
        ]
    )
    return result
