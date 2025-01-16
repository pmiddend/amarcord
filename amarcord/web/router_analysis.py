import json
from typing import Annotated
from typing import Iterable

import sqlalchemy as sa
import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import BooleanClauseList
from sqlalchemy import false
from sqlalchemy import intersect_all
from sqlalchemy import true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.orm_utils import encode_beamtime
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
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonChemicalIdAndName
from amarcord.web.json_models import JsonDataSet
from amarcord.web.json_models import JsonDataSetStatistics
from amarcord.web.json_models import JsonDataSetWithIndexingResults
from amarcord.web.json_models import JsonDetectorShift
from amarcord.web.json_models import JsonExperimentTypeWithBeamtimeInformation
from amarcord.web.json_models import JsonIndexingParametersWithResults
from amarcord.web.json_models import JsonIndexingStatistic
from amarcord.web.json_models import JsonMergeStatus
from amarcord.web.json_models import JsonReadBeamtimeGeometryDetails
from amarcord.web.json_models import JsonReadNewAnalysisInput
from amarcord.web.json_models import JsonReadNewAnalysisOutput
from amarcord.web.json_models import JsonReadRunAnalysis
from amarcord.web.json_models import JsonReadSingleDataSetResults
from amarcord.web.json_models import JsonReadSingleMergeResult
from amarcord.web.json_models import JsonRunAnalysisIndexingResult
from amarcord.web.json_models import JsonRunFile
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
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadBeamtimeGeometryDetails:
    runs = (
        await session.scalars(
            select(orm.Run)
            .where(orm.Run.beamtime_id == beamtimeId)
            .options(
                selectinload(orm.Run.indexing_results).selectinload(
                    orm.IndexingResult.indexing_parameters,
                ),
            ),
        )
    ).all()
    detector_shifts: list[JsonDetectorShift] = []
    for run in runs:
        for ir in run.indexing_results:
            if (
                ir.indexing_parameters.is_online
                and ir.detector_shift_x_mm is not None
                and ir.detector_shift_y_mm is not None
            ):
                detector_shifts.append(
                    JsonDetectorShift(
                        run_external_id=run.external_id,
                        run_start=datetime_to_attributo_int(run.started),
                        run_end=(
                            datetime_to_attributo_int(run.stopped)
                            if run.stopped is not None
                            else None
                        ),
                        shift_x_mm=ir.detector_shift_x_mm,
                        shift_y_mm=ir.detector_shift_y_mm,
                        geometry_hash=(
                            ir.geometry_hash if ir.geometry_hash is not None else ""
                        ),
                    ),
                )

    return JsonReadBeamtimeGeometryDetails(detector_shifts=detector_shifts)


@router.get(
    "/api/run-analysis/{beamtimeId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_run_analysis(
    session: Annotated[AsyncSession, Depends(get_orm_db)],
    beamtimeId: BeamtimeId,  # noqa: N803
    run_id: None | RunInternalId = None,
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
                .order_by(orm.Attributo.name),
            )
        ).all(),
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
        .options(selectinload(orm.IndexingResult.statistics)),
    )

    return JsonReadRunAnalysis(
        chemicals=[
            encode_chemical(s)
            for s in await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.beamtime_id == beamtimeId)
                .options(selectinload(orm.Chemical.files)),
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
                file_paths=[
                    JsonRunFile(id=rf.id, glob=rf.glob, source=rf.source)
                    for rf in await run.awaitable_attrs.files
                ],
            )
            if run is not None
            else None
        ),
        indexing_results=[
            JsonRunAnalysisIndexingResult(
                run_id=run.external_id if run is not None else 0,
                # foms = figures of merit
                foms=encode_indexing_fom_to_json(extract_summary(indexing_result)),
                # The only reason we're not setting this here is
                # because it's not needed for the run overview and
                # we're a bit lazy
                frames=None,
                total_frames=None,
                running=indexing_result.job_status
                in (DBJobStatus.RUNNING, DBJobStatus.QUEUED),
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
    "/api/analysis/single-data-set/{beamtimeId}/{dataSetId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_single_data_set_results(
    beamtimeId: BeamtimeId,  # noqa: N803
    dataSetId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadSingleDataSetResults:
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name),
            )
        ).all(),
    )

    chemical_id_to_name = [
        JsonChemicalIdAndName(chemical_id=x.id, name=x.name)
        for x in await session.scalars(
            select(orm.Chemical).where(orm.Chemical.beamtime_id == beamtimeId),
        )
    ]

    data_set = (
        await session.scalars(
            select(orm.DataSet)
            .join(orm.DataSet.experiment_type)
            .where(orm.DataSet.id == dataSetId),
        )
    ).one()

    # We have to explicitly get runs, because we cannot, with the DB,
    # associate runs to a data set. We have to do it in software.
    runs = {
        r.id: r
        for r in await session.scalars(
            select(orm.Run).where(
                (orm.Run.beamtime_id == beamtimeId)
                & (orm.Run.experiment_type_id == data_set.experiment_type_id),
            ),
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
            attributo_types,
            run_attributi_maps[r.id],
            ds_attributi_map,
        )
    ]
    indexing_results_for_runs: dict[RunInternalId, list[orm.IndexingResult]] = group_by(
        await session.scalars(
            select(orm.IndexingResult)
            .join(orm.Run, orm.Run.id == orm.IndexingResult.run_id)
            .where(orm.Run.beamtime_id == beamtimeId)
            .options(selectinload(orm.IndexingResult.indexing_parameters)),
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
                .where(orm.Run.beamtime_id == beamtimeId),
            ),
        )
        .options(selectinload(orm.MergeResult.indexing_results))
        .options(selectinload(orm.MergeResult.refinement_results)),
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
                beamtime_id=beamtimeId,
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
            await data_set.awaitable_attrs.experiment_type,
        ),
        data_set=_build_data_set_result(data_set),
    )


@router.get(
    "/api/analysis/merge-result/{beamtimeId}/{experimentTypeId}/{mergeResultId}",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_single_merge_result(
    beamtimeId: BeamtimeId,  # noqa: N803, ARG001
    experimentTypeId: int,  # noqa: N803
    mergeResultId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadSingleMergeResult:
    merge_result = (
        await session.scalars(
            select(orm.MergeResult)
            .where(orm.MergeResult.id == mergeResultId)
            .options(selectinload(orm.MergeResult.indexing_results))
            .options(selectinload(orm.MergeResult.refinement_results)),
        )
    ).one()

    experiment_type = encode_experiment_type(
        (
            await session.scalars(
                select(orm.ExperimentType).where(
                    orm.ExperimentType.id == experimentTypeId,
                ),
            )
        ).one(),
    )

    return JsonReadSingleMergeResult(
        result=orm_encode_merge_result_to_json(merge_result),
        experiment_type=experiment_type,
    )


@router.post(
    "/api/analysis/analysis-results",
    tags=["analysis"],
    response_model_exclude_defaults=True,
)
async def read_analysis_results(
    input_: JsonReadNewAnalysisInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadNewAnalysisOutput:
    experiment_types = list(
        (
            await session.scalars(
                select(orm.ExperimentType)
                .where(
                    orm.ExperimentType.beamtime_id == input_.beamtime_id
                    if input_.beamtime_id is not None
                    else true(),
                )
                .options(
                    selectinload(orm.ExperimentType.attributi).selectinload(
                        orm.ExperimentHasAttributo.attributo,
                    ),
                )
                .options(selectinload(orm.ExperimentType.beamtime)),
            )
        ).all(),
    )

    # One part of the response is the list of all attributi values,
    # per attributo, so we can filter based on that. Here we do a
    # distinct to filter out duplicate tuples.
    attributi_values_select = (
        select(
            orm.DataSetHasAttributoValue.attributo_id,
            orm.DataSetHasAttributoValue.integer_value,
            orm.DataSetHasAttributoValue.float_value,
            orm.DataSetHasAttributoValue.string_value,
            orm.DataSetHasAttributoValue.bool_value,
            orm.DataSetHasAttributoValue.datetime_value,
            orm.DataSetHasAttributoValue.list_value,
            orm.DataSetHasAttributoValue.chemical_value,
        )
        .join(orm.DataSetHasAttributoValue.data_set)
        .join(orm.DataSet.experiment_type)
        .where(
            # only consider attributi in experiment types just queried
            orm.DataSetHasAttributoValue.attributo_id.in_(
                [a.attributo_id for et in experiment_types for a in et.attributi],
            ),
        )
        .distinct()
    )

    attributi_values = await session.execute(attributi_values_select)

    attributi: list[orm.Attributo] = [
        et_h_a.attributo for et in experiment_types for et_h_a in et.attributi
    ]
    # for name, attributi_with_that_name in group_by(attributi, lambda a: a.name).items():
    #     primary_attributo = attributi_with_that_name[0]
    #     attributo_to_duplicates[primary_attributo]
    # attributo_to_duplicates: dict[AttributoId, set[AttributoId]] = {}

    filter_by_id: dict[int, list[JsonAttributoValue]] = group_by(
        input_.attributi_filter,
        lambda a: a.attributo_id,
    )

    attributi_groups: dict[int, set[int]] = {}
    primary_attributi: list[orm.Attributo] = []
    to_primary: dict[int, int] = {}
    attributi_seen_before: dict[tuple[str, AssociatedTable, str], int] = {}
    for a in attributi:
        attributi_groups[a.id] = set()
        primary_id = attributi_seen_before.get(
            (
                a.name,
                a.associated_table,
                json.dumps(a.json_schema),
            ),
        )
        if primary_id is None:
            primary_attributi.append(a)
            attributi_seen_before[
                (a.name, a.associated_table, json.dumps(a.json_schema))
            ] = a.id
            primary_id = a.id
        to_primary[a.id] = primary_id
        for b in attributi:
            if (
                a.name == b.name
                and a.associated_table == b.associated_table
                and a.json_schema == b.json_schema
            ):
                attributi_groups[a.id].add(b.id)

    # This function constructs a WHERE clause that looks like this:
    # attributo_id = $id AND (comparison1 OR comparison2)
    #
    # Since we want to filter with a combination of AND and OR.
    def ds_filter_clause_for_group(
        attributo_id: int,
        values: list[JsonAttributoValue],
    ) -> BooleanClauseList:
        sub_base = false()
        for value in values:
            if value.attributo_value_bool is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.bool_value
                    == value.attributo_value_bool
                )
            elif value.attributo_value_chemical is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.chemical_value
                    == value.attributo_value_chemical
                )
            elif value.attributo_value_datetime is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.datetime_value
                    == value.attributo_value_datetime
                )
            elif value.attributo_value_float is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.float_value
                    == value.attributo_value_float
                )
            elif value.attributo_value_int is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.integer_value
                    == value.attributo_value_int
                )
            elif value.attributo_value_str is not None:
                sub_base = sub_base | (
                    orm.DataSetHasAttributoValue.string_value
                    == value.attributo_value_str
                )
            else:
                raise Exception("list filters aren't supported right now")
        return (
            orm.DataSetHasAttributoValue.attributo_id.in_(
                attributi_groups[attributo_id],
            )
        ) & sub_base

    def run_filter_clause_for_group(
        attributo_id: int,
        values: list[JsonAttributoValue],
    ) -> BooleanClauseList:
        sub_base = false()
        for value in values:
            if value.attributo_value_bool is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.bool_value == value.attributo_value_bool
                )
            elif value.attributo_value_chemical is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.chemical_value
                    == value.attributo_value_chemical
                )
            elif value.attributo_value_datetime is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.datetime_value
                    == value.attributo_value_datetime
                )
            elif value.attributo_value_float is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.float_value == value.attributo_value_float
                )
            elif value.attributo_value_int is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.integer_value == value.attributo_value_int
                )
            elif value.attributo_value_str is not None:
                sub_base = sub_base | (
                    orm.RunHasAttributoValue.string_value == value.attributo_value_str
                )
            else:
                raise Exception("list filters aren't supported right now")
        return (
            orm.RunHasAttributoValue.attributo_id.in_(attributi_groups[attributo_id])
        ) & sub_base

    # This is a nasty special case: if we only have one data set, we
    # don't display any attributo filters, and the user thus cannot
    # really find any data sets. We hard-code this case here in a sort
    # of performant manner hopefully
    number_of_data_sets = await session.scalar(
        select(sa.func.count())
        .select_from(orm.DataSet)
        .where(orm.DataSet.experiment_type_id.in_(et.id for et in experiment_types))
    )
    if number_of_data_sets == 1:
        attributo_values = list(
            await session.scalars(
                select(orm.DataSetHasAttributoValue).where(
                    orm.DataSetHasAttributoValue.data_set_id.in_(
                        select(orm.DataSet.id).where(
                            orm.DataSet.experiment_type_id.in_(
                                et.id for et in experiment_types
                            )
                        )
                    )
                )
            )
        )
        filter_by_id[attributo_values[0].attributo_id] = [
            encode_data_set_attributo_value(attributo_values[0])
        ]

    filtered_data_sets: Iterable[orm.DataSet]
    filtered_runs: Iterable[orm.Run]
    if filter_by_id:
        ds_select_statement = select(orm.DataSet).where(
            orm.DataSet.id.in_(
                intersect_all(
                    *(
                        select(orm.DataSetHasAttributoValue.data_set_id)
                        .join(orm.Attributo)
                        .join(orm.DataSetHasAttributoValue.data_set)
                        .join(orm.DataSet.experiment_type)
                        .where(
                            (
                                orm.ExperimentType.beamtime_id == input_.beamtime_id
                                if input_.beamtime_id is not None
                                else true()
                            )
                            & ds_filter_clause_for_group(aid, values),
                        )
                        for aid, values in filter_by_id.items()
                    ),
                ),
            ),
        )

        filtered_data_sets = list(await session.scalars(ds_select_statement))

        run_select_statement = (
            select(orm.Run)
            .where(
                orm.Run.id.in_(
                    intersect_all(
                        *(
                            select(orm.RunHasAttributoValue.run_id)
                            .join(orm.Attributo)
                            .join(orm.RunHasAttributoValue.run)
                            .join(orm.Run.experiment_type)
                            .where(
                                (
                                    orm.ExperimentType.beamtime_id == input_.beamtime_id
                                    if input_.beamtime_id is not None
                                    else true()
                                )
                                & run_filter_clause_for_group(aid, values),
                            )
                            for aid, values in filter_by_id.items()
                        ),
                    ),
                ),
            )
            .options(
                selectinload(orm.Run.indexing_results).selectinload(
                    orm.IndexingResult.merge_results,
                ),
            )
        )

        filtered_runs = list(await session.scalars(run_select_statement))
    else:
        filtered_data_sets = []
        filtered_runs = []

    # We group runs by experiment type so the matching of data set to
    # runs is easier. We don't compare every data set with every run,
    # but only by the runs found in the experiment type.
    runs_by_experiment_type_id: dict[int, list[orm.Run]] = group_by(
        filtered_runs,
        lambda r: r.experiment_type_id,
    )

    chemical_id_to_name = [
        JsonChemicalIdAndName(chemical_id=x.id, name=x.name)
        for x in await session.scalars(
            select(orm.Chemical).where(
                orm.Chemical.beamtime_id == input_.beamtime_id
                if input_.beamtime_id is not None
                else true(),
            ),
        )
    ]

    attributo_types_by_id: dict[AttributoId, AttributoType] = {
        a.id: schema_dict_to_attributo_type(a.json_schema) for a in attributi
    }

    output_data_sets: list[JsonDataSet] = []
    output_data_set_statistics: list[JsonDataSetStatistics] = []
    for ds in filtered_data_sets:
        ds_attributi_map = {dsa.attributo_id: dsa for dsa in ds.attributo_values}
        runs_raw = runs_by_experiment_type_id.get(ds.experiment_type_id, [])
        runs_in_this_ds = [
            r
            for r in runs_raw
            if run_matches_dataset(
                attributo_types_by_id,
                {dsa.attributo_id: dsa for dsa in r.attributo_values},
                ds_attributi_map,
            )
        ]
        indexed_frames_per_run: dict[int, list[orm.IndexingResult]] = group_by(
            [
                ir
                for r in runs_in_this_ds
                for ir in r.indexing_results
                if not ir.job_error
            ],
            lambda ir: ir.run_id,
        )
        merge_results = {
            mr.id
            for r in runs_in_this_ds
            for ir in r.indexing_results
            for mr in ir.merge_results
            if mr.job_status == DBJobStatus.DONE and not mr.job_error
        }

        if (
            input_.merge_status == JsonMergeStatus.BOTH
            or (input_.merge_status == JsonMergeStatus.UNMERGED and not merge_results)
            or (input_.merge_status == JsonMergeStatus.MERGED and merge_results)
        ):
            output_data_sets.append(
                JsonDataSet(
                    id=ds.id,
                    experiment_type_id=ds.experiment_type_id,
                    attributi=[
                        encode_data_set_attributo_value(dsa)
                        for dsa in ds.attributo_values
                    ],
                    beamtime_id=ds.experiment_type.beamtime_id,
                ),
            )
            output_data_set_statistics.append(
                JsonDataSetStatistics(
                    data_set_id=ds.id,
                    run_count=len(runs_in_this_ds),
                    indexed_frames=sum(
                        max(ir.indexed_frames for ir in ir_list)
                        for ir_list in indexed_frames_per_run.values()
                    ),
                    merge_results_count=len(merge_results),
                ),
            )

    return JsonReadNewAnalysisOutput(
        searchable_attributi=[encode_attributo(a) for a in primary_attributi],
        attributi=[encode_attributo(a) for a in attributi],
        chemical_id_to_name=chemical_id_to_name,
        experiment_types=[
            JsonExperimentTypeWithBeamtimeInformation(
                experiment_type=encode_experiment_type(et),
                beamtime=encode_beamtime(et.beamtime, with_chemicals=False),
            )
            for et in experiment_types
        ],
        filtered_data_sets=output_data_sets,
        data_set_statistics=output_data_set_statistics,
        attributi_values=[
            JsonAttributoValue(
                attributo_id=to_primary[a.attributo_id],
                attributo_value_str=a.string_value,
                attributo_value_int=a.integer_value,
                attributo_value_chemical=a.chemical_value,
                attributo_value_datetime=a.datetime_value,
                attributo_value_float=a.float_value,
                attributo_value_bool=a.bool_value,
                # At some point: grab into attributo list and check
                # which type of list it is
                attributo_value_list_str=None,
                attributo_value_list_float=None,
                attributo_value_list_bool=None,
            )
            for a in attributi_values
        ],
    )
