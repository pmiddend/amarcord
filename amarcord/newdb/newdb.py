import datetime
import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

import sqlalchemy as sa
from sqlalchemy import and_
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import Label

from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue
from amarcord.newdb.beamline import Beamline
from amarcord.newdb.db_analysis_result import DBAnalysisResult
from amarcord.newdb.db_beamline_diffraction import DBBeamlineDiffraction
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_dewar_lut import DBDewarLUT
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.db_job import DBJob
from amarcord.newdb.db_puck import DBPuck
from amarcord.newdb.db_reduction_job import DBJobWithInputsAndOutputs
from amarcord.newdb.db_reduction_job import DBMiniDiffraction
from amarcord.newdb.db_reduction_job import DBMiniReduction
from amarcord.newdb.db_refinement import DBRefinement
from amarcord.newdb.db_sample_data import DBSampleData
from amarcord.newdb.db_tool import DBTool
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.reduction_simple_filter import ReductionSimpleFilter
from amarcord.newdb.refinement_method import RefinementMethod
from amarcord.newdb.tables import DBTables
from amarcord.util import DontUpdate
from amarcord.util import TriOptional
from amarcord.util import dict_union
from amarcord.workflows.command_line import parse_command_line
from amarcord.workflows.job_status import JobStatus
from amarcord.xtal_util import find_space_group_index_by_name

logger = logging.getLogger(__name__)


class NewDB:
    def __init__(self, dbcontext: DBContext, tables: DBTables) -> None:
        self.tables = tables
        self.dbcontext = dbcontext

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def crystal_exists(self, conn: Connection, crystal_id: str) -> bool:
        return (
            conn.execute(
                sa.select([self.tables.crystals.c.crystal_id]).where(
                    self.tables.crystals.c.crystal_id == crystal_id
                )
            ).fetchone()
            is not None
        )

    def has_crystals(self, conn: Connection) -> bool:
        return (
            conn.execute(sa.select([self.tables.crystals.c.crystal_id])).fetchone()
            is not None
        )

    def directory_has_reductions(self, conn: Connection, path: Path) -> bool:
        return (
            conn.execute(
                sa.select([self.tables.reductions.c.data_reduction_id]).where(
                    self.tables.reductions.c.folder_path == path
                )
            ).fetchone()
            is not None
        )

    def insert_puck(self, conn: Connection, p: DBPuck) -> None:
        conn.execute(
            sa.insert(self.tables.pucks).values(
                puck_id=p.id, puck_type=p.puck_type, owner=p.owner
            )
        )

    def insert_crystal(self, conn: Connection, c: DBCrystal) -> None:
        conn.execute(
            sa.insert(self.tables.crystals).values(
                dict_union(
                    [
                        {
                            "crystal_id": c.crystal_id,
                            "puck_id": c.puck_id,
                            "puck_position_id": c.puck_position,
                        },
                        {} if c.created is None else {"created": c.created},
                    ]
                )
            )
        )

    def has_diffractions(self, conn: Connection, crystal_id: str, run_id: int) -> bool:
        return (
            conn.execute(
                sa.select([self.tables.diffs.c.crystal_id]).where(
                    sa.and_(
                        self.tables.diffs.c.crystal_id == crystal_id,
                        self.tables.diffs.c.run_id == run_id,
                    )
                )
            ).fetchone()
            is not None
        )

    def update_diffraction(self, conn: Connection, d: DBDiffraction) -> None:
        conn.execute(
            sa.update(self.tables.diffs)
            .values(
                dict_union(
                    [
                        {
                            "dewar_position": d.dewar_position,
                            "beamline": d.beamline,
                            "beam_intensity": d.beam_intensity,
                            "pinhole": d.pinhole,
                            "focusing": d.focusing,
                            "comment": d.comment,
                            "metadata": d.metadata,
                            "angle_start": d.angle_start,
                            "exposure_time": d.exposure_time,
                            "xray_energy": d.xray_energy,
                            "xray_wavelength": d.xray_wavelength,
                            "number_of_frames": d.number_of_frames,
                            "detector_name": d.detector_name,
                            "detector_distance": d.detector_distance,
                            "detector_edge_resolution": d.detector_edge_resolution,
                            "aperture_radius": d.aperture_radius,
                            "filter_transmission": d.filter_transmission,
                            "ring_current": d.ring_current,
                            "data_raw_filename_pattern": d.data_raw_filename_pattern,
                            "microscope_image_filename_pattern": d.microscope_image_filename_pattern,
                            "aperture_horizontal": d.aperture_horizontal,
                            "aperture_vertical": d.aperture_vertical,
                        },
                        {"diffraction": d.diffraction}
                        if d.diffraction is not None
                        else {},
                        {"estimated_resolution": d.estimated_resolution}
                        if self.tables.with_estimated_resolution
                        else {},
                    ]
                )
            )
            .where(
                sa.and_(
                    self.tables.diffs.c.crystal_id == d.crystal_id,
                    self.tables.diffs.c.run_id == d.run_id,
                )
            )
        )

    def insert_diffraction(self, conn: Connection, d: DBDiffraction) -> None:
        conn.execute(
            sa.insert(self.tables.diffs).values(
                crystal_id=d.crystal_id,
                run_id=d.run_id,
                dewar_position=d.dewar_position,
                beamline=d.beamline,
                beam_intensity=d.beam_intensity,
                pinhole=d.pinhole,
                focusing=d.focusing,
                diffraction=d.diffraction,
                comment=d.comment,
                estimated_resolution=d.estimated_resolution,
                metadata=d.metadata,
                angle_start=d.angle_start,
                exposure_time=d.exposure_time,
                number_of_frames=d.number_of_frames,
                xray_energy=d.xray_energy,
                xray_wavelength=d.xray_wavelength,
                detector_name=d.detector_name,
                detector_distance=d.detector_distance,
                detector_edge_resolution=d.detector_edge_resolution,
                aperture_radius=d.aperture_radius,
                filter_transmission=d.filter_transmission,
                ring_current=d.ring_current,
                data_raw_filename_pattern=d.data_raw_filename_pattern,
                microscope_image_filename_pattern=d.microscope_image_filename_pattern,
                aperture_horizontal=d.aperture_horizontal,
                aperture_vertical=d.aperture_vertical,
            )
        )

    def insert_tool(self, conn: Connection, t: DBTool) -> int:
        assert t.id is None
        assert self.tables.tool_tables is not None

        result = conn.execute(
            sa.insert(self.tables.tool_tables.tools).values(
                name=t.name,
                executable_path=t.executable_path,
                extra_files=[str(s) for s in t.extra_files],
                command_line=t.command_line,
                description=t.description,
            )
        )
        return result.inserted_primary_key[0]

    def update_tool(self, conn: Connection, t: DBTool) -> None:
        assert self.tables.tool_tables is not None
        assert t.id is not None

        conn.execute(
            sa.update(self.tables.tool_tables.tools)
            .values(
                name=t.name,
                executable_path=t.executable_path,
                extra_files=[str(s) for s in t.extra_files],
                command_line=t.command_line,
                description=t.description,
            )
            .where(self.tables.tool_tables.tools.c.id == t.id)
        )

    def retrieve_job(self, conn: Connection, job_id: int) -> DBJob:
        assert self.tables.tool_tables is not None
        jc = self.tables.tool_tables.jobs.c
        result = conn.execute(
            sa.select(
                [
                    jc.id,
                    jc.started,
                    jc.stopped,
                    jc.failure_reason,
                    jc.queued,
                    jc.output_directory,
                    jc.tool_id,
                    jc.tool_inputs,
                    jc.metadata,
                    jc.comment,
                    jc.status,
                    jc.last_stdout,
                    jc.last_stderr,
                ]
            ).where(jc.id == job_id)
        ).one()

        return DBJob(
            job_id,
            result["queued"],
            result["status"],
            result["tool_id"],
            result["tool_inputs"],
            result["failure_reason"],
            result["comment"],
            result["output_directory"],
            result["last_stdout"],
            result["last_stderr"],
            result["metadata"],
            result["started"],
            result["stopped"],
        )

    def retrieve_jobs_with_attached(
        self,
        conn: Connection,
        limit: Optional[int] = None,
        status_filter: Optional[JobStatus] = None,
        start_at_least: Optional[datetime.datetime] = None,
    ) -> Iterable[DBJobWithInputsAndOutputs]:
        if self.tables.tool_tables is None:
            return []

        def _make_job_io(
            job: Dict[str, Any]
        ) -> Union[DBMiniReduction, DBMiniDiffraction]:
            if job["input_data_reduction_id"] is not None:
                return DBMiniReduction(
                    data_reduction_id=job["input_data_reduction_id"],
                    mtz_path=job["input_reduction_mtz_path"],
                    resolution_cc=job["input_reduction_resolution_cc"],
                    resulting_refinement_id=job["output_refinement_id"],
                    crystal_id=job["jwor_crystal_id"],
                    run_id=job["jwor_run_id"],
                )
            return DBMiniDiffraction(
                data_raw_filename_pattern=job["data_raw_filename_pattern"],
                run_id=job["jwod_run_id"],
                crystal_id=job["jwod_crystal_id"],
                resulting_data_reduction_id=job["output_data_reduction_id"],
            )

        reductions_as_result = self.tables.reductions.alias()
        reductions_as_input = self.tables.reductions.alias()

        jc = self.tables.tool_tables.jobs.c
        tc = self.tables.tool_tables.tools.c

        where_condition = and_(  # type: ignore
            True
            if status_filter is None
            else self.tables.tool_tables.jobs.c.status == status_filter,
            True
            if start_at_least is None
            else self.tables.tool_tables.jobs.c.started >= start_at_least,
        )

        # noinspection PyTypeChecker
        query_result = conn.execute(
            sa.select(
                [
                    jc.id,
                    jc.started,
                    jc.stopped,
                    jc.failure_reason,
                    jc.queued,
                    jc.output_directory,
                    jc.tool_id,
                    jc.tool_inputs,
                    jc.metadata,
                    jc.comment,
                    jc.status,
                    self.tables.tool_tables.job_working_on_diffraction.c.run_id.label(
                        "jwod_run_id"
                    ),
                    self.tables.tool_tables.job_working_on_diffraction.c.crystal_id.label(
                        "jwod_crystal_id"
                    ),
                    tc.executable_path.label("tool_executable_path"),
                    tc.name.label("tool_name"),
                    tc.description.label("tool_description"),
                    tc.command_line.label("tool_command_line"),
                    tc.extra_files.label("tool_extra_files"),
                    tc.created.label("tool_created"),
                    self.tables.diffs.c.data_raw_filename_pattern,
                    reductions_as_result.c.data_reduction_id.label(
                        "output_data_reduction_id"
                    ),
                    reductions_as_input.c.data_reduction_id.label(
                        "input_data_reduction_id"
                    ),
                    reductions_as_input.c.mtz_path.label("input_reduction_mtz_path"),
                    reductions_as_input.c.resolution_cc.label(
                        "input_reduction_resolution_cc"
                    ),
                    reductions_as_input.c.crystal_id.label("jwor_crystal_id"),
                    reductions_as_input.c.run_id.label("jwor_run_id"),
                    self.tables.tool_tables.job_has_refinement_result.c.refinement_id.label(
                        "output_refinement_id"
                    ),
                ]
            )
            .select_from(
                self.tables.tool_tables.jobs.join(self.tables.tool_tables.tools)
                .outerjoin(
                    self.tables.tool_tables.job_working_on_diffraction,
                    self.tables.tool_tables.jobs.c.id
                    == self.tables.tool_tables.job_working_on_diffraction.c.job_id,
                )
                .outerjoin(
                    self.tables.diffs,
                    sa.and_(
                        self.tables.diffs.c.crystal_id
                        == self.tables.tool_tables.job_working_on_diffraction.c.crystal_id,
                        self.tables.diffs.c.run_id
                        == self.tables.tool_tables.job_working_on_diffraction.c.run_id,
                    ),
                )
                .outerjoin(
                    self.tables.tool_tables.job_has_reduction_result,
                    self.tables.tool_tables.job_has_reduction_result.c.job_id
                    == self.tables.tool_tables.jobs.c.id,
                )
                .outerjoin(
                    reductions_as_result,
                    reductions_as_result.c.data_reduction_id
                    == self.tables.tool_tables.job_has_reduction_result.c.data_reduction_id,
                )
                .outerjoin(
                    self.tables.tool_tables.job_working_on_reduction,
                    self.tables.tool_tables.job_working_on_reduction.c.job_id
                    == self.tables.tool_tables.jobs.c.id,
                )
                .outerjoin(
                    reductions_as_input,
                    reductions_as_input.c.data_reduction_id
                    == self.tables.tool_tables.job_working_on_reduction.c.data_reduction_id,
                )
                .outerjoin(
                    self.tables.tool_tables.job_has_refinement_result,
                    self.tables.tool_tables.job_has_refinement_result.c.job_id
                    == self.tables.tool_tables.jobs.c.id,
                )
                .outerjoin(
                    self.tables.refinements,
                    self.tables.refinements.c.refinement_id
                    == self.tables.tool_tables.job_has_refinement_result.c.refinement_id,
                )
            )
            .where(where_condition)
            .order_by(jc.id.desc())
            .limit(limit)
        )
        return (
            DBJobWithInputsAndOutputs(
                DBJob(
                    id=job["id"],
                    started=job["started"],
                    stopped=job["stopped"],
                    failure_reason=job["failure_reason"],
                    comment=job["comment"],
                    queued=job["queued"],
                    status=job["status"],
                    output_directory=Path(job["output_directory"])
                    if job["output_directory"] is not None
                    else None,
                    tool_id=job["tool_id"],
                    tool_inputs=job["tool_inputs"],
                    metadata=job["metadata"],
                ),
                DBTool(
                    id=job["tool_id"],
                    name=job["tool_name"],
                    executable_path=Path(job["tool_executable_path"]),
                    extra_files=[Path(p) for p in job["tool_extra_files"]],
                    command_line=job["tool_command_line"],
                    description=job["tool_description"],
                    inputs=parse_command_line(job["tool_command_line"]),
                    created=job["tool_created"],
                ),
                io=_make_job_io(job),
            )
            for job in query_result
        )

    def update_job(
        self,
        conn: Connection,
        job_id: int,
        status: JobStatus,
        failure_reason: Optional[str],
        metadata: Optional[JSONDict],
        last_stderr: TriOptional[str] = DontUpdate(),
        last_stdout: TriOptional[str] = DontUpdate(),
        started: TriOptional[datetime.datetime] = DontUpdate(),
        stopped: TriOptional[datetime.datetime] = DontUpdate(),
        output_directory: TriOptional[Path] = DontUpdate(),
    ) -> None:
        assert self.tables.tool_tables is not None

        conn.execute(
            sa.update(self.tables.tool_tables.jobs)
            .values(
                dict_union(
                    [
                        {
                            "status": status,
                            "failure_reason": failure_reason,
                            "metadata": metadata,
                        },
                        {} if isinstance(started, DontUpdate) else {"started": started},
                        {}
                        if isinstance(last_stdout, DontUpdate)
                        else {"last_stdout": last_stdout},
                        {}
                        if isinstance(last_stderr, DontUpdate)
                        else {"last_stderr": last_stderr},
                        {} if isinstance(stopped, DontUpdate) else {"stopped": stopped},
                        {}
                        if isinstance(output_directory, DontUpdate)
                        else {"output_directory": output_directory},
                    ]
                )
            )
            .where(self.tables.tool_tables.jobs.c.id == job_id)
        )

    def insert_job_reduction_result(
        self, conn: Connection, job_id: int, data_reduction_id: int
    ) -> None:
        assert self.tables.tool_tables is not None
        conn.execute(
            sa.insert(self.tables.tool_tables.job_has_reduction_result).values(
                job_id=job_id, data_reduction_id=data_reduction_id
            )
        )

    def insert_job_refinement_result(
        self, conn: Connection, job_id: int, refinement_id: int
    ) -> None:
        assert self.tables.tool_tables is not None
        conn.execute(
            sa.insert(self.tables.tool_tables.job_has_refinement_result).values(
                job_id=job_id, refinement_id=refinement_id
            )
        )

    def insert_refinement(self, conn: Connection, r: DBRefinement) -> int:
        assert r.refinement_id is None

        result = conn.execute(
            sa.insert(self.tables.refinements).values(
                data_reduction_id=r.data_reduction_id,
                analysis_time=r.analysis_time,
                folder_path=r.folder_path,
                initial_pdb_path=r.initial_pdb_path,
                final_pdb_path=r.final_pdb_path,
                refinement_mtz_path=r.refinement_mtz_path,
                method=r.method,
                comment=r.comment,
                resolution_cut=r.resolution_cut,
                rfree=r.rfree,
                rwork=r.rwork,
                rms_bond_length=r.rms_bond_length,
                rms_bond_angle=r.rms_bond_angle,
                num_blobs=r.num_blobs,
                average_model_b=r.average_model_b,
            )
        )
        return result.inserted_primary_key[0]

    def insert_data_reduction(self, conn: Connection, r: DBDataReduction) -> int:
        assert r.data_reduction_id is None

        result = conn.execute(
            sa.insert(self.tables.reductions).values(
                crystal_id=r.crystal_id,
                run_id=r.run_id,
                analysis_time=r.analysis_time,
                folder_path=r.folder_path,
                mtz_path=r.mtz_path,
                comment=r.comment,
                method=r.method,
                resolution_cc=r.resolution_cc,
                resolution_isigma=r.resolution_isigma,
                a=r.a,
                b=r.b,
                c=r.c,
                alpha=r.alpha,
                beta=r.beta,
                gamma=r.gamma,
                space_group=find_space_group_index_by_name(r.space_group)
                if isinstance(r.space_group, str)
                else r.space_group,
                isigi=r.isigi,
                rmeas=r.rmeas,
                cchalf=r.cchalf,
                rfactor=r.rfactor,
                Wilson_b=r.wilson_b,
            )
        )
        return result.inserted_primary_key[0]

    def retrieve_tools(self, conn: Connection) -> List[DBTool]:
        if self.tables.tool_tables is None:
            return []

        tools = self.tables.tool_tables.tools
        return [
            DBTool(
                id=row["id"],
                created=row["created"],
                name=row["name"],
                executable_path=Path(row["executable_path"]),
                extra_files=row["extra_files"],
                command_line=row["command_line"],
                description=row["description"],
                inputs=parse_command_line(row["command_line"]),
            )
            for row in conn.execute(
                sa.select(
                    [
                        tools.c.id,
                        tools.c.created,
                        tools.c.name,
                        tools.c.executable_path,
                        tools.c.extra_files,
                        tools.c.command_line,
                        tools.c.description,
                    ]
                )
            ).fetchall()
        ]

    def remove_puck(self, conn: Connection, puck_id: str) -> None:
        conn.execute(
            sa.delete(self.tables.pucks).where(self.tables.pucks.c.puck_id == puck_id)
        )

    def retrieve_pucks(self, conn: Connection) -> List[DBPuck]:
        return [
            DBPuck(row["puck_id"], row["puck_type"], row["owner"])
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.pucks.c.puck_id,
                        self.tables.pucks.c.owner,
                        self.tables.pucks.c.puck_type,
                    ]
                )
            ).fetchall()
        ]

    def remove_tool(self, conn: Connection, tool_id: int) -> None:
        assert self.tables.tool_tables is not None

        conn.execute(
            sa.delete(self.tables.tool_tables.tools).where(
                self.tables.tool_tables.tools.c.id == tool_id
            )
        )

    def insert_dewar_table_entry(self, conn: Connection, d: DBDewarLUT) -> None:
        conn.execute(
            sa.insert(self.tables.dewar_lut).values(
                dewar_position=d.dewar_position, puck_id=d.puck_id
            )
        )

    def remove_dewar_table_entry(self, conn: Connection, dewar_position: int) -> None:
        conn.execute(
            sa.delete(self.tables.dewar_lut).where(
                self.tables.dewar_lut.c.dewar_position == dewar_position
            )
        )

    def truncate_dewar_table(self, conn: Connection) -> None:
        conn.execute(sa.delete(self.tables.dewar_lut))

    def retrieve_dewar_table(self, conn: Connection) -> List[DBDewarLUT]:
        return [
            DBDewarLUT(row["dewar_position"], row["puck_id"])
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.dewar_lut.c.dewar_position,
                        self.tables.dewar_lut.c.puck_id,
                    ]
                )
            ).fetchall()
        ]

    def retrieve_beamline_diffractions(
        self, conn: Connection, puck_id: str
    ) -> List[DBBeamlineDiffraction]:
        c1 = self.tables.crystals.alias()
        c2 = self.tables.crystals.alias()
        # Why this rather complicated SQL statement? The thing is, for each puck ID and position, we have multiple crystals.
        # However, for the beamline GUI, we are only interested in the latest (via date) crystal for each position.
        # And since SQL is a bit iffy w.r.t. selecting the latest value from a group, we have this rather complicated
        # statement. It selects the latest crystal IDs (for each crystal) and returns those, so we can use them in the
        # diffractions table.
        # solution comes from here:
        # https://stackoverflow.com/questions/1313120/retrieving-the-last-record-in-each-group-mysql
        # noinspection PyComparisonWithNone,PyTypeChecker
        latest_crystals = (
            sa.select([c1.c.crystal_id]).select_from(
                c1.outerjoin(
                    c2,
                    sa.and_(
                        c1.c.puck_id == c2.c.puck_id,
                        c1.c.puck_position_id == c2.c.puck_position_id,
                        c1.c.created < c2.c.created,
                    ),
                )
            )
            # pylint: disable=singleton-comparison
            .where(sa.and_(c1.c.puck_id == puck_id, c2.c.created == None))
        )
        return [
            DBBeamlineDiffraction(
                crystal_id=row["crystal_id"],
                run_id=row["run_id"],
                dewar_position=row["dewar_position"],
                diffraction=row["diffraction"],
                comment=row["comment"],
                puck_position_id=row["puck_position_id"],
                beam_intensity=row["beam_intensity"],
            )
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.crystals.c.crystal_id,
                        self.tables.diffs.c.run_id,
                        self.tables.diffs.c.dewar_position,
                        self.tables.diffs.c.diffraction,
                        self.tables.diffs.c.comment,
                        self.tables.diffs.c.beam_intensity,
                        self.tables.crystals.c.puck_position_id,
                    ]
                )
                .select_from(self.tables.crystals.outerjoin(self.tables.diffs))
                .where(self.tables.crystals.c.crystal_id.in_(latest_crystals))
                .order_by(
                    self.tables.crystals.c.puck_position_id.asc(),
                    self.tables.diffs.c.run_id.asc(),
                )
            )
        ]

    def remove_crystal(self, conn: Connection, crystal_id: str) -> None:
        conn.execute(
            sa.delete(self.tables.crystals).where(
                self.tables.crystals.c.crystal_id == crystal_id
            )
        )

    def retrieve_crystals(self, conn: Connection) -> Iterable[DBCrystal]:
        return (
            DBCrystal(
                row["crystal_id"],
                row["created"],
                row["puck_id"],
                row["puck_position_id"],
            )
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.crystals.c.crystal_id,
                        self.tables.crystals.c.created,
                        self.tables.crystals.c.puck_id,
                        self.tables.crystals.c.puck_position_id,
                    ]
                )
            ).fetchall()
        )

    def retrieve_sample_data(self, conn: Connection) -> DBSampleData:
        return DBSampleData(
            pucks=self.retrieve_pucks(conn),
            crystals=list(self.retrieve_crystals(conn)),
        )

    def retrieve_data_reductions(self, conn: Connection) -> Iterable[DBDataReduction]:
        return (
            DBDataReduction(
                data_reduction_id=r["data_reduction_id"],
                crystal_id=r["crystal_id"],
                run_id=r["run_id"],
                analysis_time=r["analysis_time"],
                folder_path=r["folder_path"],
                mtz_path=r["mtz_path"],
                method=r["method"],
                resolution_cc=r["resolution_cc"],
                comment=r["comment"],
                resolution_isigma=r["resolution_isigma"],
                a=r["a"],
                b=r["b"],
                c=r["c"],
                alpha=r["alpha"],
                beta=r["beta"],
                gamma=r["gamma"],
                space_group=r["space_group"],
                isigi=r["isigi"],
                rmeas=r["rmeas"],
                cchalf=r["cchalf"],
                rfactor=r["rfactor"],
                wilson_b=r["Wilson_b"],
            )
            for r in conn.execute(
                sa.select(["*"]).select_from(self.tables.reductions)
            ).fetchall()
        )

    def retrieve_refinements(self, conn: Connection) -> Iterable[DBRefinement]:
        return (
            DBRefinement(
                r["refinement_id"],
                r["data_reduction_id"],
                r["analysis_time"],
                r["folder_path"],
                r["method"],
                r["initial_pdb_path"],
                r["final_pdb_path"],
                r["refinement_mtz_path"],
                r["comment"],
                r["resolution_cut"],
                r["rfree"],
                r["rwork"],
                r["rms_bond_length"],
                r["rms_bond_angle"],
                r["num_blobs"],
                r["average_model_b"],
            )
            for r in conn.execute(
                sa.select(
                    [
                        # laziness ensues!
                        "*"
                    ]
                ).select_from(self.tables.refinements)
            ).fetchall()
        )

    def retrieve_analysis_diffractions(
        self, conn: Connection, filter_query: str
    ) -> Iterable[Tuple[str, int]]:
        _all_columns, query = self._analysis_filter_query(
            filter_query, sort_column=None, sort_order_desc=False
        )

        return (
            (row["diff_crystal_id"], row["diff_run_id"])
            for row in conn.execute(query)
            if row["diff_crystal_id"] is not None
        )

    def retrieve_crystal_column_values(self, conn: Connection) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for c in (
            c
            for c in self.tables.crystals.columns
            if c.name not in ("crystal_id", "created", "compound_drop_id")
        ):
            result.append(
                {
                    "columnName": c.name,
                    "values": [
                        str(x[0]) if x[0] is not None else None
                        for x in conn.execute(sa.select(c).distinct())
                    ],
                }
            )
        return result

    def retrieve_data_reduction_ids_simple(
        self,
        conn: Connection,
        filter_: ReductionSimpleFilter,
    ) -> Set[int]:
        select_from = (
            self.tables.reductions.join(
                self.tables.diffs,
                (self.tables.reductions.c.crystal_id == self.tables.diffs.c.crystal_id)
                & (self.tables.reductions.c.run_id == self.tables.diffs.c.run_id),
            )
            .join(
                self.tables.crystals,
                self.tables.crystals.c.crystal_id == self.tables.diffs.c.crystal_id,
            )
            .outerjoin(self.tables.refinements)
        )
        columns = [self.tables.reductions.c.data_reduction_id]
        query = sa.select(columns).select_from(select_from)
        if filter_.only_non_refined:
            query = query.where(self.tables.refinements.c.refinement_id.is_(None))
        if filter_.reduction_method is not None:
            query = query.where(
                self.tables.reductions.c.method == filter_.reduction_method
            )
        for column_name, value in filter_.crystal_filters.items():
            if column_name not in self.tables.crystals.columns:
                raise Exception(
                    f'crystal column "{column_name}" not found in crystals table, available columns are {self.tables.crystals.columns}'
                )
            column = self.tables.crystals.columns[column_name]
            query = query.where(column == value)
        return set(
            x[self.tables.reductions.c.data_reduction_id] for x in conn.execute(query)
        )

    def retrieve_analysis_reductions(
        self, conn: Connection, filter_query: str
    ) -> Iterable[int]:
        _all_columns, query = self._analysis_filter_query(
            filter_query, sort_column=None, sort_order_desc=False
        )

        return (
            row["dr_data_reduction_id"]
            for row in conn.execute(query)
            if row["dr_data_reduction_id"] is not None
        )

    def retrieve_analysis_results(
        self,
        conn: Connection,
        filter_query: str,
        sort_column: Optional[str],
        sort_order_desc: bool,
        limit: Optional[int],
    ) -> DBAnalysisResult:
        all_columns, query = self._analysis_filter_query(
            filter_query, sort_column, sort_order_desc
        )

        def postprocess(v: Any) -> JSONValue:
            if isinstance(v, Path):
                return str(v)
            if isinstance(v, Beamline):
                return v.value
            if isinstance(v, DiffractionType):
                return v.value
            if isinstance(v, ReductionMethod):
                return v.value
            if isinstance(v, RefinementMethod):
                return v.value
            return v

        results: List[Any] = []
        number_of_results = 0
        diffractions: Set[Tuple[str, int]] = set()
        reductions: Set[int] = set()
        for row in conn.execute(query):
            number_of_results += 1
            if limit is None or number_of_results < limit:
                results.append(row)
            diffractions.add((row["diff_crystal_id"], row["diff_run_id"]))
            if row["dr_data_reduction_id"] is not None:
                reductions.add(row["dr_data_reduction_id"])

        # noinspection PyProtectedMember
        return DBAnalysisResult(
            columns=[c.name for c in all_columns],
            rows=[
                # pylint: disable=protected-access
                [postprocess(value) for _, value in row._mapping.items()]
                for row in results
            ],
            total_diffractions=len(diffractions),
            total_reductions=len(reductions),
            total_rows=number_of_results,
        )

    def _analysis_filter_query(
        self, filter_query, sort_column, sort_order_desc
    ) -> Tuple[List[Label], Select]:
        crystal_columns = [
            c.label("crystals_" + c.name) for c in self.tables.crystals.c
        ]
        diffraction_columns = [c.label("diff_" + c.name) for c in self.tables.diffs.c]
        if not self.tables.with_estimated_resolution:
            diffraction_columns = [
                c for c in diffraction_columns if c.name != "diff_estimated_resolution"
            ]
        reduction_columns = [c.label("dr_" + c.name) for c in self.tables.reductions.c]
        refinement_columns = [
            c.label("ref_" + c.name) for c in self.tables.refinements.c
        ]
        all_columns = (
            crystal_columns
            + diffraction_columns
            + reduction_columns
            + refinement_columns
        )

        # noinspection PyTypeChecker
        select_from = self.tables.crystals.outerjoin(self.tables.diffs).outerjoin(
            self.tables.reductions,
            onclause=sa.and_(
                self.tables.reductions.c.run_id == self.tables.diffs.c.run_id,
                self.tables.reductions.c.crystal_id == self.tables.diffs.c.crystal_id,
            ),
        )
        select_from = select_from.outerjoin(
            self.tables.refinements,
            self.tables.refinements.c.data_reduction_id
            == self.tables.reductions.c.data_reduction_id,
        )
        if self.tables.tool_tables is not None:
            reduction_jobs = self.tables.tool_tables.jobs.alias()
            reduction_tools = self.tables.tool_tables.tools.alias()
            refinement_jobs = self.tables.tool_tables.jobs.alias()
            refinement_tools = self.tables.tool_tables.tools.alias()
            jwod = self.tables.tool_tables.job_working_on_diffraction
            jwor = self.tables.tool_tables.job_working_on_reduction

            all_columns += [
                reduction_jobs.c.tool_inputs.label("redjobs_tool_inputs"),
                reduction_tools.c.name.label("redjobs_tool_name"),
                refinement_jobs.c.tool_inputs.label("rfjobs_tool_inputs"),
                refinement_tools.c.name.label("rfjobs_tool_name"),
            ]

            # noinspection PyTypeChecker
            select_from = (
                select_from.outerjoin(
                    jwod,
                    sa.and_(
                        jwod.c.crystal_id == self.tables.diffs.c.crystal_id,
                        jwod.c.run_id == self.tables.diffs.c.run_id,
                    ),
                )
                .outerjoin(reduction_jobs, reduction_jobs.c.id == jwod.c.job_id)
                .outerjoin(
                    reduction_tools, reduction_tools.c.id == reduction_jobs.c.tool_id
                )
            )

            select_from = (
                select_from.outerjoin(
                    jwor,
                    jwor.c.data_reduction_id
                    == self.tables.reductions.c.data_reduction_id,
                )
                .outerjoin(refinement_jobs, refinement_jobs.c.id == jwor.c.job_id)
                .outerjoin(
                    refinement_tools, refinement_tools.c.id == refinement_jobs.c.tool_id
                )
            )
        query = (
            sa.select(all_columns).select_from(select_from).where(sa.text(filter_query))
        )
        if sort_column is not None:
            query = query.order_by(
                sa.desc(sort_column) if sort_order_desc else sort_column
            )
        query = query.distinct()
        return all_columns, query

    def insert_job(self, conn: Connection, job: DBJob) -> int:
        assert self.tables.tool_tables is not None

        result = conn.execute(
            sa.insert(self.tables.tool_tables.jobs).values(
                {
                    "queued": job.queued,
                    "started": job.started,
                    "stopped": job.stopped,
                    "status": job.status,
                    "failure_reason": job.failure_reason,
                    "output_directory": job.output_directory,
                    "comment": job.comment,
                    "tool_id": job.tool_id,
                    "tool_inputs": job.tool_inputs,
                    "metadata": job.metadata,
                }
            )
        )
        return result.inserted_primary_key[0]

    def insert_job_to_diffraction(
        self, conn: Connection, job_id: int, crystal_id: str, run_id: int
    ) -> None:
        assert self.tables.tool_tables is not None

        conn.execute(
            sa.insert(self.tables.tool_tables.job_working_on_diffraction).values(
                run_id=run_id, crystal_id=crystal_id, job_id=job_id
            )
        )

    def insert_job_to_reduction(
        self, conn: Connection, job_id: int, data_reduction_id: int
    ) -> None:
        assert self.tables.tool_tables is not None
        conn.execute(
            sa.insert(self.tables.tool_tables.job_working_on_reduction).values(
                data_reduction_id=data_reduction_id, job_id=job_id
            )
        )
