import datetime
import re
from dataclasses import dataclass
from typing import Any
from typing import Iterable
from typing import Mapping

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.orm_utils import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.orm_utils import duplicate_run_attributo
from amarcord.db.orm_utils import live_stream_image_name
from amarcord.db.orm_utils import retrieve_latest_config
from amarcord.db.orm_utils import retrieve_latest_run
from amarcord.db.orm_utils import validate_json_attributo_return_error
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.filter_expression import FilterInput
from amarcord.filter_expression import FilterParseError
from amarcord.filter_expression import compile_run_filter
from amarcord.util import group_by
from amarcord.web.fastapi_utils import DATE_FORMAT
from amarcord.web.fastapi_utils import ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS
from amarcord.web.fastapi_utils import encode_run_attributo_value
from amarcord.web.fastapi_utils import event_has_date
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import json_attributo_to_run_orm_attributo
from amarcord.web.fastapi_utils import run_has_started_date
from amarcord.web.fastapi_utils import safe_create_new_event
from amarcord.web.fastapi_utils import update_attributi_from_json
from amarcord.web.json_models import JsonAttributoBulkValue
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonCreateOrUpdateRun
from amarcord.web.json_models import JsonCreateOrUpdateRunOutput
from amarcord.web.json_models import JsonIndexingStatistic
from amarcord.web.json_models import JsonReadRuns
from amarcord.web.json_models import JsonReadRunsBulkInput
from amarcord.web.json_models import JsonReadRunsBulkOutput
from amarcord.web.json_models import JsonRun
from amarcord.web.json_models import JsonRunAnalysisIndexingResult
from amarcord.web.json_models import JsonStartRunOutput
from amarcord.web.json_models import JsonStopRunOutput
from amarcord.web.json_models import JsonUpdateRun
from amarcord.web.json_models import JsonUpdateRunOutput
from amarcord.web.json_models import JsonUpdateRunsBulkInput
from amarcord.web.json_models import JsonUpdateRunsBulkOutput
from amarcord.web.router_attributi import encode_attributo
from amarcord.web.router_chemicals import encode_chemical
from amarcord.web.router_data_sets import encode_data_set
from amarcord.web.router_events import encode_event
from amarcord.web.router_experiment_types import encode_experiment_type
from amarcord.web.router_indexing import encode_summary
from amarcord.web.router_indexing import fom_for_indexing_result
from amarcord.web.router_indexing import summary_from_foms
from amarcord.web.router_user_configuration import encode_user_configuration

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()
_SHIFT_RE = re.compile(r"(\d{2}):(\d{2})-(\d{2}):(\d{2})")


def extract_runs_and_event_dates(
    runs: Iterable[orm.Run], events: Iterable[orm.EventLog]
) -> list[str]:
    set_of_dates: set[str] = set()
    for run in runs:
        started_date = run.started
        set_of_dates.add(started_date.strftime(DATE_FORMAT))
    for event in events:
        set_of_dates.add(event.created.strftime(DATE_FORMAT))

    return sorted(set_of_dates, reverse=True)


def indexing_fom_for_run(
    indexing_results_for_runs: Mapping[RunInternalId, list[orm.IndexingResult]],
    run: orm.Run,
) -> DBIndexingFOM:
    indexing_results_for_run = sorted(
        [
            (ir.id, fom_for_indexing_result(ir))
            for ir in indexing_results_for_runs.get(run.id, [])
            if not ir.job_error
        ],
        key=lambda ir: ir[0],
        reverse=True,
    )
    if indexing_results_for_run:
        return indexing_results_for_run[0][1]
    return empty_indexing_fom


@router.get(
    "/api/runs/{runExternalId}/start/{beamtimeId}",
    tags=["runs"],
    response_model_exclude_defaults=True,
)
async def start_run(
    runExternalId: RunExternalId,
    beamtimeId: BeamtimeId,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonStartRunOutput:
    async with session.begin():
        latest_config = await retrieve_latest_config(session, beamtimeId)
        experiment_type_id = latest_config.current_experiment_type_id
        if experiment_type_id is None:
            raise HTTPException(
                status_code=400, detail="Cannot create run, no experiment type set!"
            )
        new_run = orm.Run(
            external_id=runExternalId,
            experiment_type_id=experiment_type_id,
            beamtime_id=beamtimeId,
            started=datetime.datetime.utcnow(),
            modified=datetime.datetime.utcnow(),
        )
        if latest_config.auto_pilot:
            latest_run = await retrieve_latest_run(session, beamtimeId)
            if latest_run is not None:
                for latest_run_attributo in latest_run.attributo_values:
                    if (
                        await latest_run_attributo.awaitable_attrs.attributo
                    ).group == ATTRIBUTO_GROUP_MANUAL:
                        new_run.attributo_values.append(
                            duplicate_run_attributo(latest_run_attributo)
                        )
        session.add(new_run)
        await session.flush()
        return JsonStartRunOutput(run_internal_id=new_run.id)


@router.get(
    "/api/runs/stop-latest/{beamtimeId}",
    tags=["runs"],
    response_model_exclude_defaults=True,
)
async def stop_latest_run(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
) -> JsonStopRunOutput:
    async with session.begin():
        latest_run = await retrieve_latest_run(session, beamtimeId)

        if latest_run is not None:
            latest_run.stopped = datetime.datetime.utcnow()
            await session.commit()
            return JsonStopRunOutput(result=True)

        return JsonStopRunOutput(result=False)


@router.post(
    "/api/runs/{runExternalId}", tags=["runs"], response_model_exclude_defaults=True
)
async def create_or_update_run(
    runExternalId: RunExternalId,
    input_: JsonCreateOrUpdateRun,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonCreateOrUpdateRunOutput:
    beamtime_id = input_.beamtime_id
    run_logger = logger.bind(run_external_id=runExternalId, beamtime_id=beamtime_id)
    run_logger.info("creating (or updating) run")

    async with session.begin():
        latest_config = await retrieve_latest_config(session, beamtime_id)
        run_logger.info(
            f"retrieved latest user config: {latest_config.use_online_crystfel}"
        )
        experiment_type_id = latest_config.current_experiment_type_id
        if experiment_type_id is None:
            run_logger.error("no experiment type set, cannot create run")
            raise HTTPException(
                status_code=400, detail="Cannot create run, no experiment type set!"
            )
        run_logger.info(f"experiment type set to {experiment_type_id}")
        run_in_db = (
            await session.scalars(
                select(orm.Run).where(
                    (orm.Run.external_id == runExternalId)
                    & (orm.Run.beamtime_id == beamtime_id)
                )
            )
        ).one_or_none()
        run_was_created = run_in_db is None
        if run_in_db is None:
            run_logger.info("run not in DB, creating a new one")
            run_in_db = orm.Run(
                external_id=runExternalId,
                experiment_type_id=experiment_type_id,
                beamtime_id=beamtime_id,
                started=(
                    datetime.datetime.utcnow()
                    if input_.started is None
                    else datetime_from_attributo_int(input_.started)
                ),
                stopped=(
                    None
                    if input_.stopped is None
                    else datetime_from_attributo_int(input_.stopped)
                ),
                modified=datetime.datetime.utcnow(),
            )
            attributi_by_id: dict[int, orm.Attributo] = {
                a.id: a
                for a in (
                    await session.scalars(
                        select(orm.Attributo).where(
                            orm.Attributo.id.in_(
                                a.attributo_id for a in input_.attributi
                            )
                            & (orm.Attributo.associated_table == AssociatedTable.RUN)
                        )
                    )
                )
            }
            for new_attributo in input_.attributi:
                attributo_type = attributi_by_id.get(new_attributo.attributo_id)
                if attributo_type is None:
                    raise HTTPException(
                        status_code=400,
                        detail=f"attributo with ID {new_attributo.attributo_id} not found in list of run attributi",
                    )
                validation_result = validate_json_attributo_return_error(
                    new_attributo, attributo_type
                )
                run_logger.info(
                    f"validating type of {new_attributo.attributo_id}: type is {attributo_type}: {validation_result}"
                )
                if validation_result is not None:
                    raise HTTPException(
                        status_code=400,
                        detail=f"error validating attributi: {validation_result}",
                    )
                run_in_db.attributo_values.append(
                    json_attributo_to_run_orm_attributo(new_attributo)
                )
            if latest_config.auto_pilot:
                latest_run = await retrieve_latest_run(session, beamtime_id)
                if latest_run is not None:
                    for latest_run_attributo in latest_run.attributo_values:
                        if (
                            await latest_run_attributo.awaitable_attrs.attributo
                        ).group == ATTRIBUTO_GROUP_MANUAL:
                            run_in_db.attributo_values.append(
                                duplicate_run_attributo(latest_run_attributo)
                            )
            session.add(run_in_db)
        else:
            run_logger.info("run in DB, updating attributes")
            await update_attributi_from_json(
                session,
                db_item=run_in_db,
                new_attributi=input_.attributi,
                attributi_by_id={
                    a.id: a
                    for a in (
                        await session.scalars(
                            select(orm.Attributo).where(
                                orm.Attributo.id.in_(
                                    a.attributo_id for a in input_.attributi
                                )
                                & (
                                    orm.Attributo.associated_table
                                    == AssociatedTable.RUN
                                )
                            )
                        )
                    )
                },
            )
            if input_.started is not None:
                run_in_db.started = datetime_from_attributo_int(input_.started)
            if input_.stopped is not None:
                run_in_db.stopped = datetime_from_attributo_int(input_.stopped)

        async def _inner_create_new_event(text: str) -> None:
            run_logger.error(text)
            await safe_create_new_event(
                logger,
                session,
                beamtime_id,
                f"run {runExternalId}: {text}",
                EventLogLevel.INFO,
                "API",
            )

        if not run_was_created:
            run_logger.info("CrystFEL online not needed, run is updated, not created")
            indexing_result_id = None
        elif not latest_config.use_online_crystfel:
            run_logger.info(
                "CrystFEL online deactivated (or never explicitly activated), not creating indexing job"
            )
            indexing_result_id = None
        else:
            run_logger.info("adding CrystFEL online job")
            attributi = list(
                (
                    await session.scalars(
                        select(orm.Attributo)
                        .where(orm.Attributo.beamtime_id == beamtime_id)
                        .order_by(orm.Attributo.name)
                    )
                ).all()
            )
            point_group_attributo = next(
                iter(a for a in attributi if a.name == "point group"), None
            )
            if point_group_attributo is None:
                message = "cannot start CrystFEL online: have no point group attributo"
                await _inner_create_new_event(message)
                raise HTTPException(
                    status_code=400,
                    detail=message,
                )
            cell_description_attributo = next(
                iter(a for a in attributi if a.name == "cell description"), None
            )
            if cell_description_attributo is None:
                message = (
                    "cannot start CrystFEL online: have no cell description attributo"
                )
                await _inner_create_new_event(message)
                raise HTTPException(
                    status_code=400,
                    detail=message,
                )
            point_group: None | str = None
            cell_description_str: None | str = None
            channel_chemical: None | orm.Chemical = None
            # For indexing, we need to provide one chemical ID that serves as _the_ chemical ID for the indexing job
            # (kind of a bug right now). So, if we don't find any chemicals with cell information, we just use the first
            # one which is of type "crystal". Since it's totally valid to leave out cell information for crystals, for
            # example in the case where you actually don't know that and want to find out.
            crystal_chemicals: list[orm.Chemical] = []
            for channel in range(1, ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS + 1):
                run_logger.info(
                    f"run attributo values are: {run_in_db.attributo_values}"
                )
                async for this_channel_chemical in (
                    (
                        await session.scalars(
                            select(orm.Chemical).where(
                                orm.Chemical.id == attributo_value.chemical_value
                            )
                        )
                    ).one()
                    for attributo_value in run_in_db.attributo_values
                    if attributo_value.chemical_value is not None
                    and attributo_value.attributo.name
                    == f"channel_{channel}_chemical_id"
                ):
                    run_logger.info(
                        f"got a chemical for the channel {channel}: {this_channel_chemical.id}"
                    )
                    if this_channel_chemical.type == ChemicalType.CRYSTAL:
                        crystal_chemicals.append(this_channel_chemical)
                    this_point_group = next(
                        iter(
                            attributo_value.string_value
                            for attributo_value in this_channel_chemical.attributo_values
                            if attributo_value.attributo_id == point_group_attributo.id
                        ),
                        None,
                    )
                    this_cell_description = next(
                        iter(
                            attributo_value.string_value
                            for attributo_value in this_channel_chemical.attributo_values
                            if attributo_value.attributo_id
                            == cell_description_attributo.id
                        ),
                        None,
                    )
                    if (
                        this_point_group is not None
                        and this_cell_description is not None
                    ):
                        point_group = this_point_group
                        cell_description_str = this_cell_description
                        channel_chemical = this_channel_chemical
                        break

            if channel_chemical is None:
                if not crystal_chemicals:
                    error_message = (
                        "cannot start CrystFEL online: no chemicals with cell information and none "
                        + 'of type "crystal" detected'
                    )
                    await _inner_create_new_event(error_message)
                    run_logger.warning(error_message)
                    return JsonCreateOrUpdateRunOutput(
                        run_created=False,
                        indexing_result_id=None,
                        error_message=error_message,
                        run_internal_id=None,
                    )
                channel_chemical = crystal_chemicals[0]
                info_message = (
                    "no chemicals with cell information found, taking the first chemical of type "
                    + f' "crystal": {channel_chemical.name} (id {channel_chemical.id})'
                )
                await _inner_create_new_event(info_message)
                run_logger.info(info_message)

            cell_description: None | CrystFELCellFile
            if cell_description_str is not None:
                cell_description = parse_cell_description(cell_description_str)
                if cell_description is None:
                    error_message = f"cannot start indexing job, cell description is invalid: {cell_description_str}"
                    await _inner_create_new_event(error_message)
                    logger.error(error_message)
                    return JsonCreateOrUpdateRunOutput(
                        run_created=False,
                        indexing_result_id=None,
                        error_message=error_message,
                        run_internal_id=None,
                    )
            else:
                cell_description = None

            run_logger.info(
                f"creating CrystFEL online job for chemical {channel_chemical}"
            )
            # Better to explicitly flush, creating the run and giving us the ID
            await session.flush()
            new_indexing_result = orm.IndexingResult(
                created=datetime.datetime.utcnow(),
                run_id=run_in_db.id,
                frames=0,
                hit_rate=0.0,
                indexing_rate=0.0,
                hits=0,
                not_indexed_frames=0,
                indexed_frames=0,
                job_status=DBJobStatus.QUEUED,
                point_group=(
                    point_group
                    if point_group is not None and point_group.strip()
                    else None
                ),
                cell_description=(
                    coparse_cell_description(cell_description)
                    if cell_description is not None
                    else None
                ),
                chemical_id=channel_chemical.id,
            )
            session.add(new_indexing_result)
            await session.flush()
            indexing_result_id = new_indexing_result.id

    return JsonCreateOrUpdateRunOutput(
        run_created=run_was_created,
        indexing_result_id=indexing_result_id,
        error_message=None,
        run_internal_id=run_in_db.id,
    )


@router.patch("/api/runs", tags=["runs"], response_model_exclude_defaults=True)
async def update_run(
    input_: JsonUpdateRun, session: AsyncSession = Depends(get_orm_db)
) -> JsonUpdateRunOutput:
    async with session.begin():
        run_id = RunInternalId(input_.id)
        current_run = (
            await session.scalars(select(orm.Run).where(orm.Run.id == run_id))
        ).one()
        await update_attributi_from_json(
            session,
            db_item=current_run,
            new_attributi=input_.attributi,
            attributi_by_id={
                a.id: a
                for a in (
                    await session.scalars(
                        select(orm.Attributo).where(
                            orm.Attributo.id.in_(
                                a.attributo_id for a in input_.attributi
                            )
                            & (orm.Attributo.associated_table == AssociatedTable.RUN)
                        )
                    )
                )
            },
        )
        current_run.experiment_type_id = input_.experiment_type_id
        await session.commit()

    return JsonUpdateRunOutput(result=True)


def encode_attributo_value(
    attributo_id: int, attributo_value: AttributoValue
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=attributo_id,
        attributo_value_str=(
            attributo_value if isinstance(attributo_value, str) else None
        ),
        attributo_value_int=(
            attributo_value if isinstance(attributo_value, int) else None
        ),
        attributo_value_float=(
            attributo_value if isinstance(attributo_value, float) else None
        ),
        attributo_value_bool=(
            attributo_value if isinstance(attributo_value, bool) else None
        ),
        # we cannot thoroughly test the array for type-correctness (or we dont' want to, rather)
        attributo_value_list_str=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], str))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
        attributo_value_list_float=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], (int, float)))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
        attributo_value_list_bool=(
            attributo_value
            if isinstance(attributo_value, list)
            and (not attributo_value or isinstance(attributo_value[0], bool))
            else None
        ),  # pyright: ignore[reportGeneralTypeIssues]
    )


@dataclass(frozen=True)
class _RunHasAttributoValueToBeUsedInSet:
    integer_value: None | int
    float_value: None | float
    string_value: None | str
    bool_value: None | bool
    datetime_value: None | datetime.datetime
    list_value: None | list[Any]
    chemical_value: None | int


def _to_dataclass(o: orm.RunHasAttributoValue) -> _RunHasAttributoValueToBeUsedInSet:
    return _RunHasAttributoValueToBeUsedInSet(
        integer_value=o.integer_value,
        float_value=o.float_value,
        string_value=o.string_value,
        bool_value=o.bool_value,
        datetime_value=o.datetime_value,
        list_value=o.list_value,
        chemical_value=o.chemical_value,
    )


def _encode_dataclass(
    id_: AttributoId, r: _RunHasAttributoValueToBeUsedInSet
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=id_,
        attributo_value_str=r.string_value,
        attributo_value_int=r.integer_value,
        attributo_value_chemical=r.chemical_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(r.datetime_value)
            if r.datetime_value is not None
            else None
        ),
        attributo_value_float=r.float_value,
        attributo_value_bool=r.bool_value,
        attributo_value_list_str=(
            r.list_value if r.list_value and isinstance(r.list_value[0], str) else None
        ),
        attributo_value_list_float=(
            r.list_value
            if r.list_value and isinstance(r.list_value[0], (float, int))
            else None
        ),
        attributo_value_list_bool=(
            r.list_value if r.list_value and isinstance(r.list_value[0], bool) else None
        ),
    )


@router.post("/api/runs-bulk", tags=["runs"], response_model_exclude_defaults=True)
async def read_runs_bulk(
    input_: JsonReadRunsBulkInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonReadRunsBulkOutput:
    beamtime_id = input_.beamtime_id
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtime_id)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )
    chemicals = (
        await session.scalars(
            select(orm.Chemical)
            .where(orm.Chemical.beamtime_id == beamtime_id)
            .options(selectinload(orm.Chemical.files))
        )
    ).all()
    all_runs = (
        await session.scalars(
            select(orm.Run).where(
                # Important! Since we're using the external run ID for this request, we need to constrain the
                # beamtime ID here, since two beamtimes will surely have overlapping run IDs
                (orm.Run.external_id.in_(input_.external_run_ids))
                & (orm.Run.beamtime_id == beamtime_id)
            )
        )
    ).all()
    bulk_attributi: dict[AttributoId, set[_RunHasAttributoValueToBeUsedInSet]] = {
        a.id: set() for a in attributi
    }
    for r in all_runs:
        for a in r.attributo_values:
            bulk_attributi[a.attributo_id].add(_to_dataclass(a))
    return JsonReadRunsBulkOutput(
        chemicals=[encode_chemical(s) for s in chemicals],
        attributi=[
            encode_attributo(a)
            for a in attributi
            if a.associated_table == AssociatedTable.RUN
        ],
        attributi_values=[
            JsonAttributoBulkValue(
                attributo_id=attributo_id,
                values=[_encode_dataclass(attributo_id, v) for v in values],
            )
            for attributo_id, values in bulk_attributi.items()
        ],
        experiment_types=[
            encode_experiment_type(a)
            for a in (
                await session.scalars(
                    select(orm.ExperimentType).where(
                        orm.ExperimentType.beamtime_id == beamtime_id
                    )
                )
            ).all()
        ],
        experiment_type_ids=list(set(r.experiment_type_id for r in all_runs)),
    )


@router.patch("/api/runs-bulk", tags=["runs"], response_model_exclude_defaults=True)
async def update_runs_bulk(
    input_: JsonUpdateRunsBulkInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonUpdateRunsBulkOutput:
    async with session.begin():
        for run in await session.scalars(
            select(orm.Run).where(
                (orm.Run.beamtime_id == input_.beamtime_id)
                & (orm.Run.external_id.in_(input_.external_run_ids))
            )
        ):
            if input_.new_experiment_type_id is not None:
                run.experiment_type_id = input_.new_experiment_type_id
                await update_attributi_from_json(
                    session,
                    db_item=run,
                    new_attributi=input_.attributi,
                    attributi_by_id={
                        a.id: a
                        for a in (
                            await session.scalars(
                                select(orm.Attributo).where(
                                    orm.Attributo.id.in_(
                                        a.attributo_id for a in input_.attributi
                                    )
                                    & (
                                        orm.Attributo.associated_table
                                        == AssociatedTable.RUN
                                    )
                                )
                            )
                        )
                    },
                )
        return JsonUpdateRunsBulkOutput(result=True)


async def _find_schedule_entry(
    session: AsyncSession, beamtime_id: BeamtimeId
) -> None | orm.BeamtimeSchedule:
    now = datetime.datetime.now()
    minutes_since_midnight_now = now.hour * 60 + now.minute
    for schedule_entry in await session.scalars(
        select(orm.BeamtimeSchedule).where(
            (orm.BeamtimeSchedule.beamtime_id == beamtime_id)
        )
    ):
        entry_match = _SHIFT_RE.search(schedule_entry.shift)
        if entry_match is not None:
            minutes_since_midnight_from = int(entry_match.group(1)) * 60 + int(
                entry_match.group(2)
            )
            minutes_since_midnight_to = int(entry_match.group(3)) * 60 + int(
                entry_match.group(4)
            )
            if (
                minutes_since_midnight_from
                <= minutes_since_midnight_now
                <= minutes_since_midnight_to
            ):
                return schedule_entry
    return None


@router.get(
    "/api/runs/{beamtimeId}", tags=["runs"], response_model_exclude_defaults=True
)
async def read_runs(
    beamtimeId: BeamtimeId,
    date: None | str = None,
    # pylint: disable=redefined-builtin
    filter: None | str = None,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadRuns:
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo)
                .where(orm.Attributo.beamtime_id == beamtimeId)
                .order_by(orm.Attributo.name)
            )
        ).all()
    )
    chemicals = (
        await session.scalars(
            select(orm.Chemical)
            .where(orm.Chemical.beamtime_id == beamtimeId)
            .options(selectinload(orm.Chemical.files))
        )
    ).all()
    experiment_types = (
        await session.scalars(
            select(orm.ExperimentType).where(
                orm.ExperimentType.beamtime_id == beamtimeId
            )
        )
    ).all()
    data_sets = (
        await session.scalars(
            select(orm.DataSet, orm.ExperimentType)
            .join(orm.DataSet.experiment_type)
            .where(orm.ExperimentType.beamtime_id == beamtimeId)
        )
    ).all()
    all_runs = (
        await session.scalars(
            select(orm.Run).where(orm.Run.beamtime_id == beamtimeId)
            # Sort by inverse chronological order
            .order_by(orm.Run.started.desc())
        )
    ).all()
    all_events = (
        await session.scalars(
            select(orm.EventLog)
            .where(
                (orm.EventLog.beamtime_id == beamtimeId)
                & (orm.EventLog.level == EventLogLevel.USER)
            )
            .order_by(orm.EventLog.created.desc())
            .options(selectinload(orm.EventLog.files))
        )
    ).all()

    attributo_name_to_id: dict[str, AttributoId] = {
        a.name: AttributoId(a.id)
        for a in attributi
        if a.associated_table == AssociatedTable.RUN
    }

    try:
        run_filter = compile_run_filter(filter.strip() if filter is not None else "")
        runs = [
            run
            for run in all_runs
            if run_filter(
                FilterInput(
                    run=run,
                    chemical_names={s.name: s.id for s in chemicals},
                    attributo_name_to_id=attributo_name_to_id,
                )
            )
        ]
    except FilterParseError as e:
        raise Exception(f"error in filter string: {e}")

    date_filter = date.strip() if date is not None else ""
    runs = (
        [run for run in runs if run_has_started_date(run, date_filter)]
        if date_filter
        else runs
    )
    events = (
        [event for event in all_events if event_has_date(event, date_filter)]
        if date_filter
        else all_events
    )

    indexing_results = (
        await session.scalars(
            select(orm.IndexingResult, orm.Run)
            .join(orm.IndexingResult.run)
            .where(orm.Run.beamtime_id == beamtimeId)
        )
    ).all()
    indexing_results_for_runs: dict[RunInternalId, list[orm.IndexingResult]] = group_by(
        indexing_results, lambda ir: ir.run_id
    )
    run_foms: dict[RunInternalId, DBIndexingFOM] = {
        r.id: indexing_fom_for_run(indexing_results_for_runs, r) for r in runs
    }
    attributo_types: dict[AttributoId, AttributoType] = {
        AttributoId(a.id): schema_dict_to_attributo_type(a.json_schema)
        for a in attributi
    }
    run_attributi_maps: dict[
        int,
        dict[AttributoId, None | orm.RunHasAttributoValue],
    ] = {r.id: {ra.attributo_id: ra for ra in r.attributo_values} for r in all_runs}
    data_set_attributi_maps: dict[
        int,
        dict[AttributoId, None | orm.DataSetHasAttributoValue],
    ] = {
        ds.id: {dsa.attributo_id: dsa for dsa in ds.attributo_values}
        for ds in data_sets
    }
    data_set_id_to_grouped: dict[int, DBIndexingFOM] = {
        ds.id: summary_from_foms(
            [
                run_foms.get(r.id, empty_indexing_fom)
                for r in runs
                if r.experiment_type_id == ds.experiment_type_id
                and run_matches_dataset(
                    attributo_types,
                    run_attributi_maps[r.id],
                    data_set_attributi_maps[ds.id],
                )
            ]
        )
        for ds in data_sets
    }

    user_configuration = await retrieve_latest_config(session, beamtimeId)
    live_stream_file = (
        await session.scalars(
            select(orm.File).where(
                orm.File.file_name == live_stream_image_name(beamtimeId)
            )
        )
    ).one_or_none()
    if all_runs:
        latest_run = all_runs[0]
        latest_indexing_results = await latest_run.awaitable_attrs.indexing_results
        if latest_indexing_results:
            latest_indexing_result_orm = latest_indexing_results[0]
            latest_statistics_orm = (
                await latest_indexing_result_orm.awaitable_attrs.statistics
            )
            latest_indexing_result = JsonRunAnalysisIndexingResult(
                run_id=latest_run.id,
                foms=[],
                indexing_statistics=[
                    JsonIndexingStatistic(
                        time=datetime_to_attributo_int(stat.time),
                        frames=stat.frames,
                        hits=stat.hits,
                        indexed=stat.indexed_frames,
                        crystals=stat.indexed_crystals,
                    )
                    for stat in latest_statistics_orm
                ],
            )
        else:
            latest_indexing_result = None
    else:
        latest_indexing_result = None
    found_schedule_entry = await _find_schedule_entry(session, beamtimeId)
    return JsonReadRuns(
        current_beamtime_user=(
            None if found_schedule_entry is None else found_schedule_entry.users
        ),
        latest_indexing_result=latest_indexing_result,
        live_stream_file_id=(
            live_stream_file.id if live_stream_file is not None else None
        ),
        filter_dates=extract_runs_and_event_dates(all_runs, all_events),
        attributi=[encode_attributo(a) for a in attributi],
        events=[encode_event(e) for e in events],
        chemicals=[encode_chemical(a) for a in chemicals],
        user_config=encode_user_configuration(user_configuration),
        experiment_types=[encode_experiment_type(a) for a in experiment_types],
        data_sets=[
            encode_data_set(a, data_set_id_to_grouped.get(a.id, None))
            for a in data_sets
        ],
        runs=[
            JsonRun(
                id=r.id,
                external_id=r.external_id,
                attributi=[encode_run_attributo_value(v) for v in r.attributo_values],
                started=datetime_to_attributo_int(r.started),
                stopped=(
                    datetime_to_attributo_int(r.stopped)
                    if r.stopped is not None
                    else None
                ),
                files=[],
                summary=encode_summary(run_foms.get(r.id, empty_indexing_fom)),
                experiment_type_id=r.experiment_type_id,
                data_sets=[
                    ds.id
                    for ds in data_sets
                    if r.experiment_type_id == ds.experiment_type_id
                    and run_matches_dataset(
                        attributo_types,
                        run_attributi_maps[r.id],
                        data_set_attributi_maps[ds.id],
                    )
                ],
                running_indexing_jobs=[
                    ir.id
                    for ir in indexing_results
                    if ir.run_id == r.id and ir.job_status == DBJobStatus.RUNNING
                ],
            )
            for r in runs
        ],
    )
