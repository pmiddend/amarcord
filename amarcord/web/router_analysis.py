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
from amarcord.web.fastapi_utils import encode_run_attributo_value
from amarcord.web.fastapi_utils import format_run_id_intervals
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonAnalysisDataSet
from amarcord.web.json_models import JsonAnalysisExperimentType
from amarcord.web.json_models import JsonAnalysisRun
from amarcord.web.json_models import JsonChemicalIdAndName
from amarcord.web.json_models import JsonIndexingStatistic
from amarcord.web.json_models import JsonReadAnalysisResults
from amarcord.web.json_models import JsonReadRunAnalysis
from amarcord.web.json_models import JsonRunAnalysisIndexingResult
from amarcord.web.router_attributi import encode_attributo
from amarcord.web.router_chemicals import encode_chemical
from amarcord.web.router_data_sets import encode_data_set
from amarcord.web.router_experiment_types import encode_experiment_type
from amarcord.web.router_indexing import encode_summary
from amarcord.web.router_indexing import fom_for_indexing_result
from amarcord.web.router_indexing import summary_from_foms
from amarcord.web.router_merging import encode_merge_result
from amarcord.web.router_runs import indexing_fom_for_run

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


@router.get(
    "/api/run-analysis/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_run_analysis(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
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
    return JsonReadRunAnalysis(
        chemicals=[
            encode_chemical(s)
            for s in await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.beamtime_id == beamtimeId)
                .options(selectinload(orm.Chemical.files))
            )
        ],
        attributi=[encode_attributo(a) for a in attributi],
        runs=[
            JsonAnalysisRun(
                id=r.external_id,
                attributi=[encode_run_attributo_value(v) for v in r.attributo_values],
            )
            for r in runs
        ],
        indexing_results_by_run_id=[
            JsonRunAnalysisIndexingResult(
                run_id=indexing_results_for_run[0].run.external_id,
                # foms = figures of merit
                foms=[
                    encode_summary(extract_summary(indexing_result))
                    for indexing_result in indexing_results_for_run
                ],
                indexing_statistics=[
                    JsonIndexingStatistic(
                        time=datetime_to_attributo_int(stat.time),
                        frames=stat.frames,
                        hits=stat.hits,
                        indexed=stat.indexed_frames,
                        crystals=stat.indexed_crystals,
                    )
                    for ir in indexing_results_for_run
                    for stat in ir.statistics
                ],
            )
            for indexing_results_for_run in group_by(
                (
                    await session.scalars(
                        select(orm.IndexingResult)
                        .join(orm.Run, orm.IndexingResult.run_id == orm.Run.id)
                        .where(orm.Run.beamtime_id == beamtimeId)
                        .options(selectinload(orm.IndexingResult.statistics))
                    )
                ),
                lambda ir: ir.run_id,
            ).values()
        ],
    )


@router.get(
    "/api/analysis/analysis-results/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_analysis_results(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
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

    data_sets = list(
        (
            await session.scalars(
                select(orm.DataSet)
                .join(orm.DataSet.experiment_type)
                .where(orm.ExperimentType.beamtime_id == beamtimeId)
            )
        ).all()
    )
    data_sets_by_experiment_type: dict[int, list[orm.DataSet]] = group_by(
        data_sets,
        lambda ds: ds.experiment_type_id,
    )
    # We have to explicitly get runs, because we cannot, with the DB, associate runs to a data set. We have to do it in software.
    runs = {
        r.id: r
        for r in await session.scalars(
            select(orm.Run).where(orm.Run.beamtime_id == beamtimeId)
        )
    }
    # To output external run IDs in the merge results, instead of internal ones
    # (hacky and probably more easily doable)
    run_internal_to_external_id = {r.id: r.external_id for r in runs.values()}
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
    indexing_results_for_runs: dict[RunInternalId, list[orm.IndexingResult]] = group_by(
        await session.scalars(
            select(orm.IndexingResult)
            .join(orm.Run, orm.Run.id == orm.IndexingResult.run_id)
            .where(orm.Run.beamtime_id == beamtimeId)
        ),
        lambda ir: ir.run_id,
    )
    merge_results_per_data_set: dict[int, list[orm.MergeResult]] = {
        ds.id: [] for ds in data_sets
    }
    data_set_to_run_ids: dict[int, set[int]] = {
        ds_id: set(r.id for r in runs) for ds_id, runs in data_set_to_runs.items()
    }
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
        for ds_id, run_ids in data_set_to_run_ids.items():
            if run_ids.issuperset(runs_in_merge_result):
                merge_results_per_data_set[ds_id].append(merge_result)
    run_foms: dict[RunInternalId, DBIndexingFOM] = {
        r.id: indexing_fom_for_run(indexing_results_for_runs, r) for r in runs.values()
    }

    def _build_data_set_result(
        ds: orm.DataSet, merge_results: list[orm.MergeResult]
    ) -> JsonAnalysisDataSet:
        runs_in_ds = data_set_to_runs.get(ds.id, [])
        return JsonAnalysisDataSet(
            data_set=encode_data_set(
                ds,
                summary_from_foms(
                    [
                        run_foms.get(r.id, empty_indexing_fom)
                        for r in runs.values()
                        if r.experiment_type_id == ds.experiment_type_id
                        and run_matches_dataset(
                            attributo_types,
                            run_attributi_maps[r.id],
                            ds_attributi_maps[ds.id],
                        )
                    ]
                ),
            ),
            runs=format_run_id_intervals(r.external_id for r in runs_in_ds),
            number_of_indexing_results=sum(
                len(indexing_results_for_runs.get(run.id, [])) for run in runs_in_ds
            ),
            merge_results=[
                encode_merge_result(
                    mr,
                    run_id_formatter=lambda rid: run_internal_to_external_id[rid],
                )
                for mr in merge_results
            ],
        )

    return JsonReadAnalysisResults(
        attributi=[encode_attributo(a) for a in attributi],
        chemical_id_to_name=[
            JsonChemicalIdAndName(chemical_id=x.id, name=x.name)
            for x in await session.scalars(
                select(orm.Chemical).where(orm.Chemical.beamtime_id == beamtimeId)
            )
        ],
        experiment_types=[
            encode_experiment_type(et)
            for et in await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.beamtime_id == beamtimeId
                )
            )
        ],
        data_sets=[
            JsonAnalysisExperimentType(
                experiment_type=et_id,
                data_sets=[
                    _build_data_set_result(
                        ds, merge_results_per_data_set.get(ds.id, [])
                    )
                    for ds in data_sets
                ],
            )
            for et_id, data_sets in data_sets_by_experiment_type.items()
        ],
    )
