import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.run_internal_id import RunInternalId
from amarcord.util import group_by
from amarcord.web.fastapi_utils import encode_data_set_attributo_value
from amarcord.web.fastapi_utils import encode_run_attributo_value
from amarcord.web.fastapi_utils import format_run_id_intervals
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import orm_encode_merge_result_to_json
from amarcord.web.fastapi_utils import orm_indexing_parameters_to_json
from amarcord.web.fastapi_utils import orm_indexing_result_to_json
from amarcord.web.json_models import JsonAnalysisRun
from amarcord.web.json_models import JsonChemicalIdAndName
from amarcord.web.json_models import JsonDataSet
from amarcord.web.json_models import JsonDataSetWithIndexingResults
from amarcord.web.json_models import JsonDataSetWithoutIndexingResults
from amarcord.web.json_models import JsonDetectorShift
from amarcord.web.json_models import JsonIndexingParametersWithResults
from amarcord.web.json_models import JsonIndexingStatistic
from amarcord.web.json_models import JsonReadAnalysisResults
from amarcord.web.json_models import JsonReadBeamtimeGeometryDetails
from amarcord.web.json_models import JsonReadRunAnalysis
from amarcord.web.json_models import JsonReadSingleDataSetResults
from amarcord.web.json_models import JsonReadSingleMergeResult
from amarcord.web.json_models import JsonRunAnalysisIndexingResult
from amarcord.web.json_models import JsonRunId
from amarcord.web.router_attributi import encode_attributo
from amarcord.web.router_chemicals import encode_chemical
from amarcord.web.router_experiment_types import encode_experiment_type
from amarcord.web.router_indexing import encode_indexing_fom_to_json
from amarcord.web.router_indexing import fom_for_indexing_result

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


@router.get(
    "/api/run-analysis/{beamtimeId}/geometry",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_beamtime_geometry_details(
    beamtimeId: BeamtimeId,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadBeamtimeGeometryDetails:
    runs = (
        await session.scalars(
            select(orm.Run)
            .where((orm.Run.beamtime_id == beamtimeId))
            .options(selectinload(orm.Run.indexing_results))
        )
    ).all()
    detector_shifts: list[JsonDetectorShift] = []
    for run in runs:
        for ir in run.indexing_results:
            if (
                ir.detector_shift_x_mm is not None
                and ir.detector_shift_y_mm is not None
            ):
                detector_shifts.append(
                    JsonDetectorShift(
                        run_external_id=run.external_id,
                        shift_x_mm=ir.detector_shift_x_mm,
                        shift_y_mm=ir.detector_shift_y_mm,
                    )
                )

    return JsonReadBeamtimeGeometryDetails(detector_shifts=detector_shifts)


@router.get(
    "/api/run-analysis/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_run_analysis(
    beamtimeId: BeamtimeId,
    run_id: None | RunInternalId = None,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadRunAnalysis:
    def extract_summary(o: orm.IndexingResult) -> DBIndexingFOM:
        if o.job_error is not None:
            return empty_indexing_fom
        return fom_for_indexing_result(o)

    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )
    runs = (
        await session.scalars(select(orm.Run).where(orm.Run.beamtime_id == beamtimeId))
    ).all()
    run: None | orm.Run = None
    if run_id is not None:
        for r in runs:
            if r.id == run_id:
                run = r
                break
    indexing_results = await session.scalars(
        select(orm.IndexingResult)
        .where(orm.IndexingResult.run_id == run_id)
        .options(selectinload(orm.IndexingResult.statistics))
    )

    return JsonReadRunAnalysis(
        chemicals=[
            encode_chemical(s)
            for s in await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.beamtime_id == beamtimeId)
                .options(selectinload(orm.Chemical.files))
            )
        ],
        run_ids=[
            JsonRunId(internal_run_id=r.id, external_run_id=r.external_id) for r in runs
        ],
        attributi=[encode_attributo(a) for a in attributi],
        run=(
            JsonAnalysisRun(
                id=run.id,
                external_id=run.external_id,
                attributi=[encode_run_attributo_value(v) for v in run.attributo_values],
            )
            if run is not None
            else None
        ),
        indexing_results=[
            JsonRunAnalysisIndexingResult(
                run_id=run.external_id if run is not None else 0,
                # foms = figures of merit
                foms=encode_indexing_fom_to_json(extract_summary(indexing_result)),
                indexing_statistics=[
                    JsonIndexingStatistic(
                        time=datetime_to_attributo_int(stat.time),
                        frames=stat.frames,
                        hits=stat.hits,
                        indexed=stat.indexed_frames,
                        crystals=stat.indexed_crystals,
                    )
                    for stat in indexing_result.statistics
                ],
            )
            for indexing_result in indexing_results
        ],
    )


@router.get(
    "/api/analysis/analysis-results/{beamtimeId}/{experimentTypeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_analysis_results(
    beamtimeId: BeamtimeId,
    experimentTypeId: int,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadAnalysisResults:
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )

    chemical_id_to_name = [
        JsonChemicalIdAndName(chemical_id=x.id, name=x.name)
        for x in await session.scalars(
            select(orm.Chemical).where(orm.Chemical.beamtime_id == beamtimeId)
        )
    ]

    experiment_type = encode_experiment_type(
        (
            await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.id == experimentTypeId
                )
            )
        ).one()
    )

    data_sets = list(
        (
            await session.scalars(
                select(orm.DataSet)
                .join(orm.DataSet.experiment_type)
                .where(
                    (orm.ExperimentType.beamtime_id == beamtimeId)
                    & (orm.ExperimentType.id == experimentTypeId)
                )
            )
        ).all()
    )
    # We have to explicitly get runs, because we cannot, with the DB,
    # associate runs to a data set. We have to do it in software.
    runs = {
        r.id: r
        for r in await session.scalars(
            select(orm.Run).where(
                (orm.Run.beamtime_id == beamtimeId)
                & (orm.Run.experiment_type_id == experimentTypeId)
            )
        )
    }
    run_attributi_maps: dict[
        RunInternalId,
        dict[AttributoId, None | orm.RunHasAttributoValue],
    ] = {
        r.id: {ra.attributo_id: ra for ra in r.attributo_values} for r in runs.values()
    }
    ds_attributi_maps: dict[
        int,
        dict[AttributoId, None | orm.DataSetHasAttributoValue],
    ] = {
        ds.id: {dsa.attributo_id: dsa for dsa in ds.attributo_values}
        for ds in data_sets
    }
    attributo_types: dict[AttributoId, AttributoType] = {
        a.id: schema_dict_to_attributo_type(a.json_schema) for a in attributi
    }

    data_set_to_runs: dict[int, list[orm.Run]] = {
        ds.id: [
            r
            for r in runs.values()
            if r.experiment_type_id == ds.experiment_type_id
            and run_matches_dataset(
                attributo_types,
                run_attributi_maps[r.id],
                ds_attributi_maps[ds.id],
            )
        ]
        for ds in data_sets
    }

    def _build_data_set_result(ds: orm.DataSet) -> JsonDataSetWithoutIndexingResults:
        runs_in_ds: list[orm.Run] = data_set_to_runs.get(ds.id, [])

        return JsonDataSetWithoutIndexingResults(
            data_set=JsonDataSet(
                id=ds.id,
                experiment_type_id=ds.experiment_type_id,
                attributi=[
                    encode_data_set_attributo_value(v) for v in ds.attributo_values
                ],
            ),
            internal_run_ids=[r.id for r in runs_in_ds],
            runs=format_run_id_intervals(r.external_id for r in runs_in_ds),
        )

    return JsonReadAnalysisResults(
        attributi=[encode_attributo(a) for a in attributi],
        chemical_id_to_name=chemical_id_to_name,
        experiment_type=experiment_type,
        data_sets=[_build_data_set_result(ds) for ds in data_sets],
    )


@router.get(
    "/api/analysis/single-data-set/{beamtimeId}/{dataSetId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_single_data_set_results(
    beamtimeId: BeamtimeId,
    dataSetId: int,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadSingleDataSetResults:
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )

    chemical_id_to_name = [
        JsonChemicalIdAndName(chemical_id=x.id, name=x.name)
        for x in await session.scalars(
            select(orm.Chemical).where(orm.Chemical.beamtime_id == beamtimeId)
        )
    ]

    data_set = (
        await session.scalars(
            select(orm.DataSet)
            .join(orm.DataSet.experiment_type)
            .where(orm.DataSet.id == dataSetId)
        )
    ).one()

    # We have to explicitly get runs, because we cannot, with the DB,
    # associate runs to a data set. We have to do it in software.
    runs = {
        r.id: r
        for r in await session.scalars(
            select(orm.Run).where(
                (orm.Run.beamtime_id == beamtimeId)
                & (orm.Run.experiment_type_id == data_set.experiment_type_id)
            )
        )
    }
    run_external_id_for_internal_id: dict[int, int] = {
        r.id: r.external_id for r in runs.values()
    }
    run_attributi_maps: dict[
        RunInternalId,
        dict[AttributoId, None | orm.RunHasAttributoValue],
    ] = {
        r.id: {ra.attributo_id: ra for ra in r.attributo_values} for r in runs.values()
    }
    ds_attributi_map: dict[AttributoId, None | orm.DataSetHasAttributoValue] = {
        dsa.attributo_id: dsa for dsa in data_set.attributo_values
    }
    attributo_types: dict[AttributoId, AttributoType] = {
        a.id: schema_dict_to_attributo_type(a.json_schema) for a in attributi
    }

    relevant_runs: list[orm.Run] = [
        r
        for r in runs.values()
        if r.experiment_type_id == data_set.experiment_type_id
        and run_matches_dataset(
            attributo_types, run_attributi_maps[r.id], ds_attributi_map
        )
    ]
    indexing_results_for_runs: dict[RunInternalId, list[orm.IndexingResult]] = group_by(
        await session.scalars(
            select(orm.IndexingResult)
            .join(orm.Run, orm.Run.id == orm.IndexingResult.run_id)
            .where(orm.Run.beamtime_id == beamtimeId)
            .options(selectinload(orm.IndexingResult.indexing_parameters))
        ),
        lambda ir: ir.run_id,
    )
    indexing_parameter_ids = set(
        x.indexing_parameters_id
        for run_results in indexing_results_for_runs.values()
        for x in run_results
    )
    merge_results_per_data_set: dict[int, list[orm.MergeResult]] = {
        ip_id: [] for ip_id in indexing_parameter_ids
    }
    data_set_run_ids: set[int] = set(r.id for r in relevant_runs)

    # Super hard to wrap your head around this, so let me explain:
    #
    # A merge result has a list of indexing results. Each indexing result belongs to exactly one run.
    # Thus, for each merge result, we can get a list of runs.
    #
    # A data set has a list of runs matching it.
    #
    # We now define: A merge result matches a data set if the data set's runs are a strict superset of the merge
    # result's runs.
    #
    # This is what's tested here.
    for merge_result in await session.scalars(
        select(orm.MergeResult)
        .where(
            orm.MergeResult.id.in_(
                select(orm.MergeResult.id)
                .join(orm.MergeResult.indexing_results)
                .join(orm.Run, orm.Run.id == orm.IndexingResult.run_id)
                .where((orm.Run.beamtime_id == beamtimeId))
            )
        )
        .options(selectinload(orm.MergeResult.indexing_results))
        .options(selectinload(orm.MergeResult.refinement_results))
    ):
        runs_in_merge_result: set[RunInternalId] = set(
            ir.run_id for ir in merge_result.indexing_results
        )
        if data_set_run_ids.issuperset(runs_in_merge_result):
            for ir in merge_result.indexing_results:
                merge_results = merge_results_per_data_set[ir.indexing_parameters_id]
                if merge_result.id not in (mr.id for mr in merge_results):
                    merge_results.append(merge_result)

    def _build_data_set_result(ds: orm.DataSet) -> JsonDataSetWithIndexingResults:
        # In the code that follows: ip is "indexing parameters"
        ip_and_ix_results: list[
            tuple[orm.IndexingParameters, list[orm.IndexingResult]]
        ] = []

        for run in relevant_runs:
            for new_result in indexing_results_for_runs.get(run.id, []):
                new_ip = new_result.indexing_parameters
                # Check if this parameter is already captured. If so,
                # add result to list Complexity is too high here I
                # realize. Hashing the orm.IndexingParameters would be
                # advantageous, but I'm not sure about hash and
                # compatibility with sqlalchemy. Would have to be an
                # external hash.
                ip_is_really_new = True
                for existing_ip, existing_result in ip_and_ix_results:
                    if orm.are_indexing_parameters_equal(existing_ip, new_ip):
                        existing_result.append(new_result)
                        ip_is_really_new = False
                        break

                if ip_is_really_new:
                    ip_and_ix_results.append((new_ip, [new_result]))

        return JsonDataSetWithIndexingResults(
            data_set=JsonDataSet(
                id=ds.id,
                experiment_type_id=ds.experiment_type_id,
                attributi=[
                    encode_data_set_attributo_value(v) for v in ds.attributo_values
                ],
            ),
            internal_run_ids=[r.id for r in relevant_runs],
            runs=format_run_id_intervals(r.external_id for r in relevant_runs),
            indexing_results=[
                JsonIndexingParametersWithResults(
                    parameters=orm_indexing_parameters_to_json(ip),
                    indexing_results=[
                        orm_indexing_result_to_json(result) for result in results
                    ],
                    merge_results=[
                        orm_encode_merge_result_to_json(
                            mr,
                            run_id_formatter=lambda id: run_external_id_for_internal_id[
                                id
                            ],
                        )
                        for mr in merge_results_per_data_set.get(ip.id, [])
                    ],
                )
                for ip, results in ip_and_ix_results
            ],
        )

    return JsonReadSingleDataSetResults(
        attributi=[encode_attributo(a) for a in attributi],
        chemical_id_to_name=chemical_id_to_name,
        experiment_type=encode_experiment_type(
            await data_set.awaitable_attrs.experiment_type
        ),
        data_set=_build_data_set_result(data_set),
    )


@router.get(
    "/api/analysis/merge-result/{beamtimeId}/{experimentTypeId}/{mergeResultId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_single_merge_result(
    # pylint: disable=unused-argument
    beamtimeId: BeamtimeId,
    experimentTypeId: int,
    mergeResultId: int,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadSingleMergeResult:
    merge_result = (
        await session.scalars(
            select(orm.MergeResult)
            .where(orm.MergeResult.id == mergeResultId)
            .options(selectinload(orm.MergeResult.indexing_results))
            .options(selectinload(orm.MergeResult.refinement_results))
        )
    ).one()

    experiment_type = encode_experiment_type(
        (
            await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.id == experimentTypeId
                )
            )
        ).one()
    )

    return JsonReadSingleMergeResult(
        result=orm_encode_merge_result_to_json(merge_result),
        experiment_type=experiment_type,
    )
