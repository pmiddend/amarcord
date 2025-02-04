import datetime
import json
import os
from typing import Any
from typing import AsyncGenerator
from typing import Callable
from typing import Iterable
from typing import cast

import structlog
from fastapi import HTTPException
from sqlalchemy import NullPool
from sqlalchemy import StaticPool
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.merge_result import JsonMergeResultFom
from amarcord.db.merge_result import JsonMergeResultInternal
from amarcord.db.merge_result import JsonMergeResultOuterShell
from amarcord.db.merge_result import JsonMergeResultShell
from amarcord.db.merge_result import JsonRefinementResultInternal
from amarcord.db.orm_utils import validate_json_attributo_return_error
from amarcord.db.run_internal_id import RunInternalId
from amarcord.util import create_intervals
from amarcord.web.constants import DATE_FORMAT
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonIndexingParameters
from amarcord.web.json_models import JsonIndexingResult
from amarcord.web.json_models import JsonMergeParameters
from amarcord.web.json_models import JsonMergeResult
from amarcord.web.json_models import JsonMergeResultStateDone
from amarcord.web.json_models import JsonMergeResultStateError
from amarcord.web.json_models import JsonMergeResultStateQueued
from amarcord.web.json_models import JsonMergeResultStateRunning
from amarcord.web.json_models import JsonPolarisation
from amarcord.web.json_models import JsonRefinementResult
from amarcord.web.json_models import JsonRunRange


def _json_serializer_allow_nan_false(obj: Any, **kwargs: Any) -> str:
    return json.dumps(obj, **kwargs, allow_nan=False)


def get_orm_sessionmaker_with_url(db_url: str) -> async_sessionmaker[AsyncSession]:
    # For the sqlite in-memory stuff, see here:
    #
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
    #
    # In short, with in-memory databases, we normally get one DB per thread, which is bad if we have
    # background threads updating the DB (in test scenarios, of course).
    in_memory_db = db_url == "sqlite+aiosqlite://"

    engine = create_async_engine(
        db_url,
        echo="DB_ECHO" in os.environ,
        connect_args={"check_same_thread": False} if in_memory_db else {},
        poolclass=StaticPool if in_memory_db else NullPool,
        json_serializer=_json_serializer_allow_nan_false,
    )

    # sqlite doesn't care about foreign keys unless you do this dance, see
    # https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
    def _fk_pragma_on_connect(dbapi_con: Any, _con_record: Any) -> None:
        dbapi_con.execute("pragma foreign_keys=ON")

    if "sqlite" in db_url:
        event.listen(engine.sync_engine, "connect", _fk_pragma_on_connect)

    return async_sessionmaker(engine, expire_on_commit=False)


async def get_orm_db() -> AsyncGenerator[AsyncSession, None]:
    async_session = get_orm_sessionmaker_with_url(os.environ["DB_URL"])

    async with async_session() as session:
        yield session


def json_attributo_to_data_set_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.DataSetHasAttributoValue:
    return orm.DataSetHasAttributoValue(
        attributo_id=AttributoId(new_attributo.attributo_id),
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
        chemical_value=new_attributo.attributo_value_chemical,
    )


def json_attributo_to_run_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.RunHasAttributoValue:
    return orm.RunHasAttributoValue(
        attributo_id=AttributoId(new_attributo.attributo_id),
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
        chemical_value=new_attributo.attributo_value_chemical,
    )


def json_attributo_to_chemical_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.ChemicalHasAttributoValue:
    return orm.ChemicalHasAttributoValue(
        attributo_id=new_attributo.attributo_id,
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
    )


async def update_attributi_from_json(
    session: AsyncSession,
    db_item: orm.Run | orm.Chemical | orm.DataSet,
    attributi_by_id: dict[int, orm.Attributo],
    new_attributi: list[JsonAttributoValue],
) -> None:
    attributo_ids_to_update = set(x.attributo_id for x in new_attributi)
    for existing_attributo in db_item.attributo_values:
        if existing_attributo.attributo_id in attributo_ids_to_update:
            await session.delete(existing_attributo)
    for new_attributo in new_attributi:
        validation_result = validate_json_attributo_return_error(
            new_attributo,
            attributi_by_id[new_attributo.attributo_id],
        )
        if validation_result is not None:
            raise HTTPException(
                status_code=400,
                detail=f"error validating attributi: {validation_result}",
            )
        if isinstance(db_item, orm.Run):
            db_item.attributo_values.append(
                json_attributo_to_run_orm_attributo(new_attributo),
            )
        elif isinstance(db_item, orm.Chemical):
            db_item.attributo_values.append(
                json_attributo_to_chemical_orm_attributo(new_attributo),
            )
        else:
            db_item.attributo_values.append(
                json_attributo_to_data_set_orm_attributo(new_attributo),
            )


def format_run_id_intervals(run_ids: Iterable[int]) -> list[str]:
    return [
        str(t[0]) if t[0] == t[1] else f"{t[0]}-{t[1]}"
        for t in create_intervals(list(run_ids))
    ]


def run_id_to_run_ranges(run_ids: Iterable[int]) -> list[JsonRunRange]:
    return [
        JsonRunRange(run_from=t[0], run_to=t[1])
        for t in create_intervals(list(run_ids))
    ]


async def safe_create_new_event(
    this_logger: structlog.stdlib.BoundLogger,
    session: AsyncSession,
    beamtime_id: BeamtimeId,
    text: str,
    level: EventLogLevel,
    source: str,
) -> None:
    try:
        session.add(
            orm.EventLog(
                beamtime_id=beamtime_id,
                level=level,
                source=source,
                text=text,
                created=datetime.datetime.now(datetime.timezone.utc),
            ),
        )
    except:
        this_logger.exception("error writing event log")


def run_has_started_date(run: orm.Run, date_filter: str) -> bool:
    return run.started.strftime(DATE_FORMAT) == date_filter


def event_has_date(e: orm.EventLog, date_filter: str) -> bool:
    return e.created.strftime(DATE_FORMAT) == date_filter


def encode_data_set_attributo_value(
    d: orm.DataSetHasAttributoValue,
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=d.attributo_id,
        attributo_value_str=d.string_value,
        attributo_value_int=d.integer_value,
        attributo_value_chemical=d.chemical_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(d.datetime_value)
            if d.datetime_value is not None
            else None
        ),
        attributo_value_float=d.float_value,
        attributo_value_bool=d.bool_value,
        attributo_value_list_str=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], str)  # type: ignore
            else None
        ),
        attributo_value_list_float=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], float)  # type: ignore
            else None
        ),
        attributo_value_list_bool=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], bool)  # type: ignore
            else None
        ),
    )


def encode_run_attributo_value(
    d: orm.RunHasAttributoValue,
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=d.attributo_id,
        attributo_value_str=d.string_value,
        attributo_value_int=d.integer_value,
        attributo_value_chemical=d.chemical_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(d.datetime_value)
            if d.datetime_value is not None
            else None
        ),
        attributo_value_float=d.float_value,
        attributo_value_bool=d.bool_value,
        attributo_value_list_str=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], str)  # type: ignore
            else None
        ),
        attributo_value_list_float=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], float)  # type: ignore
            else None
        ),
        attributo_value_list_bool=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], bool)  # type: ignore
            else None
        ),
    )


def orm_encode_json_merge_parameters_to_json(
    mr: orm.MergeResult,
) -> JsonMergeParameters:
    return JsonMergeParameters(
        point_group=mr.point_group,
        space_group=mr.space_group,
        cell_description=mr.cell_description,
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


def orm_encode_merge_result_to_json(
    mr: orm.MergeResult,
    run_id_formatter: None | Callable[[RunInternalId], int] = None,
) -> JsonMergeResult:
    return JsonMergeResult(
        id=mr.id,
        created=datetime_to_attributo_int(mr.created),
        indexing_result_ids=[ir.id for ir in mr.indexing_results],
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
                    mtz_file_id=cast(int, mr.mtz_file_id),
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
        parameters=orm_encode_json_merge_parameters_to_json(mr),
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


async def retrieve_runs_matching_data_set(
    session: AsyncSession,
    data_set_id: int,
    beamtime_id: int,
) -> list[orm.Run]:
    data_set = (
        await session.scalars(select(orm.DataSet).where(orm.DataSet.id == data_set_id))
    ).one_or_none()
    if data_set is None:
        raise HTTPException(
            status_code=400,
            detail=f'Data set with ID "{data_set_id}" not found',
        )
    all_runs = (
        await session.scalars(select(orm.Run).where(orm.Run.beamtime_id == beamtime_id))
    ).all()
    attributi = list(
        (
            await session.scalars(
                select(orm.Attributo).where(orm.Attributo.beamtime_id == beamtime_id),
            )
        ).all(),
    )
    attributo_types: dict[AttributoId, AttributoType] = {
        AttributoId(a.id): schema_dict_to_attributo_type(a.json_schema)
        for a in attributi
    }
    run_attributi_maps: dict[
        int,
        dict[AttributoId, None | orm.RunHasAttributoValue],
    ] = {r.id: {ra.attributo_id: ra for ra in r.attributo_values} for r in all_runs}
    data_set_attributi_map = {
        dsa.attributo_id: dsa for dsa in data_set.attributo_values
    }
    return [
        r
        for r in all_runs
        if r.experiment_type_id == data_set.experiment_type_id
        and run_matches_dataset(
            attributo_types,
            run_attributi_maps[r.id],
            data_set_attributi_map,
        )
    ]


def orm_indexing_parameters_to_json(
    p: orm.IndexingParameters,
) -> JsonIndexingParameters:
    return JsonIndexingParameters(
        id=p.id,
        is_online=p.is_online,
        cell_description=p.cell_description,
        command_line=p.command_line,
        geometry_file=p.geometry_file if p.geometry_file is not None else "",
    )


def orm_indexing_result_to_json(ir: orm.IndexingResult) -> JsonIndexingResult:
    ip = ir.indexing_parameters
    return JsonIndexingResult(
        id=ir.id,
        created=datetime_to_attributo_int(ir.created),
        parameters=JsonIndexingParameters(
            id=ip.id,
            is_online=ip.is_online,
            cell_description=ip.cell_description,
            command_line=ip.command_line,
            geometry_file=ip.geometry_file if ip.geometry_file is not None else "",
        ),
        program_version=ir.program_version if ir.program_version is not None else "",
        run_internal_id=ir.run_id,
        run_external_id=ir.run.external_id,
        frames=0 if ir.frames is None else ir.frames,
        hits=0 if ir.hits is None else ir.hits,
        indexed_frames=ir.indexed_frames,
        indexed_crystals=ir.indexed_frames,
        has_error=bool(ir.job_error),
        detector_shift_x_mm=ir.detector_shift_x_mm,
        detector_shift_y_mm=ir.detector_shift_y_mm,
        geometry_file=ir.geometry_file if ir.geometry_file is not None else "",
        geometry_hash=ir.geometry_hash if ir.geometry_hash is not None else "",
        generated_geometry_file=(
            "" if ir.generated_geometry_file is None else ir.generated_geometry_file
        ),
        status=ir.job_status,
        started=(
            datetime_to_attributo_int(ir.job_started)
            if ir.job_started is not None
            else None
        ),
        stopped=(
            datetime_to_attributo_int(ir.job_stopped)
            if ir.job_stopped is not None
            else None
        ),
        unit_cell_histograms_file_id=ir.unit_cell_histograms_file_id,
    )
