import datetime
from pathlib import Path
from typing import Any
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import sqlalchemy as sa
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import Label

from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONArray
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue
from amarcord.newdb.beamline import Beamline
from amarcord.newdb.db_analysis_result import DBAnalysisResult
from amarcord.newdb.db_analysis_row import DBAnalysisRow
from amarcord.newdb.db_beamline_diffraction import DBBeamlineDiffraction
from amarcord.newdb.db_crystal import DBCrystal
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_dewar_lut import DBDewarLUT
from amarcord.newdb.db_diffraction import DBDiffraction
from amarcord.newdb.db_job import DBJob
from amarcord.newdb.db_puck import DBPuck
from amarcord.newdb.db_reduction_job import DBReductionJob
from amarcord.newdb.db_sample_data import DBSampleData
from amarcord.newdb.db_tool import DBTool
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.tables import DBTables
from amarcord.util import DontUpdate
from amarcord.util import TriOptional
from amarcord.util import dict_union
from amarcord.workflows.command_line import CommandLine
from amarcord.workflows.command_line import parse_command_line
from amarcord.workflows.job_status import JobStatus
from amarcord.xtal_util import find_space_group_index_by_name


def _convert_command_line(c: CommandLine) -> JSONArray:
    return [{"name": x.name, "type": x.type_.value} for x in c.inputs]


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
                    self.tables.reductions.c.folder_path == str(path)
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
                            "estimated_resolution": d.estimated_resolution,
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
                            "data_raw_filename_pattern": str(
                                d.data_raw_filename_pattern
                            ),
                            "microscope_image_filename_pattern": str(
                                d.microscope_image_filename_pattern
                            ),
                            "aperture_horizontal": d.aperture_horizontal,
                            "aperture_vertical": d.aperture_vertical,
                        },
                        {"diffraction": d.diffraction}
                        if d.diffraction is not None
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
                data_raw_filename_pattern=str(d.data_raw_filename_pattern)
                if d.data_raw_filename_pattern is not None
                else None,
                microscope_image_filename_pattern=str(
                    d.microscope_image_filename_pattern
                )
                if d.microscope_image_filename_pattern is not None
                else None,
                aperture_horizontal=d.aperture_horizontal,
                aperture_vertical=d.aperture_vertical,
            )
        )

    def insert_tool(self, conn: Connection, t: DBTool) -> int:
        result = conn.execute(
            sa.insert(self.tables.tools).values(
                name=t.name,
                executable_path=str(t.executable_path),
                extra_files=[str(s) for s in t.extra_files],
                command_line=t.command_line,
                description=t.description,
            )
        )
        return result.inserted_primary_key[0]

    def update_tool(self, conn: Connection, t: DBTool) -> None:
        assert t.id is not None
        conn.execute(
            sa.update(self.tables.tools)
            .values(
                name=t.name,
                executable_path=str(t.executable_path),
                extra_files=[str(s) for s in t.extra_files],
                command_line=t.command_line,
                description=t.description,
            )
            .where(self.tables.tools.c.id == t.id)
        )

    def retrieve_reduction_jobs(self, conn: Connection) -> List[DBReductionJob]:
        return [
            DBReductionJob(
                id=job["id"],
                started=job["started"],
                stopped=job["stopped"],
                failure_reason=job["failure_reason"],
                queued=job["queued"],
                status=job["status"],
                output_directory=Path(job["output_directory"])
                if job["output_directory"] is not None
                else None,
                data_raw_filename_pattern=job["data_raw_filename_pattern"],
                run_id=job["run_id"],
                crystal_id=job["crystal_id"],
                metadata=job["metadata"],
                tool=DBTool(
                    id=job["tool_id"],
                    name=job["name"],
                    description=job["description"],
                    executable_path=Path(job["executable_path"]),
                    extra_files=[Path(p) for p in job["extra_files"]],
                    command_line=job["command_line"],
                    inputs=[],
                ),
                tool_inputs=job["tool_inputs"],
            )
            for job in conn.execute(
                sa.select(
                    [
                        self.tables.jobs.c.id,
                        self.tables.jobs.c.started,
                        self.tables.jobs.c.stopped,
                        self.tables.jobs.c.failure_reason,
                        self.tables.jobs.c.queued,
                        self.tables.jobs.c.output_directory,
                        self.tables.jobs.c.tool_id,
                        self.tables.jobs.c.tool_inputs,
                        self.tables.jobs.c.metadata,
                        self.tables.jobs.c.status,
                        self.tables.reduction_jobs.c.run_id,
                        self.tables.reduction_jobs.c.crystal_id,
                        self.tables.tools.c.executable_path,
                        self.tables.tools.c.name,
                        self.tables.tools.c.description,
                        self.tables.tools.c.command_line,
                        self.tables.tools.c.extra_files,
                        self.tables.diffs.c.data_raw_filename_pattern,
                    ]
                ).select_from(
                    self.tables.jobs.join(self.tables.reduction_jobs)
                    .join(self.tables.tools)
                    .join(self.tables.diffs)
                )
            )
        ]

    def update_job(
        self,
        conn: Connection,
        job_id: int,
        status: JobStatus,
        failure_reason: Optional[str],
        metadata: Optional[JSONDict],
        started: TriOptional[datetime.datetime] = DontUpdate(),
        stopped: TriOptional[datetime.datetime] = DontUpdate(),
        output_directory: TriOptional[Path] = DontUpdate(),
    ) -> None:
        conn.execute(
            sa.update(self.tables.jobs)
            .values(
                dict_union(
                    [
                        {
                            "status": status,
                            "failure_reason": failure_reason,
                            "metadata": metadata,
                        },
                        {} if isinstance(started, DontUpdate) else {"started": started},
                        {} if isinstance(stopped, DontUpdate) else {"stopped": stopped},
                        {}
                        if isinstance(output_directory, DontUpdate)
                        else {
                            "output_directory": str(output_directory)
                            if output_directory is not None
                            else None
                        },
                    ]
                )
            )
            .where(self.tables.jobs.c.id == job_id)
        )

    def insert_job_reduction_result(
        self, conn: Connection, job_id: int, data_reduction_id: int
    ) -> None:
        conn.execute(
            sa.insert(self.tables.job_reductions).values(
                job_id=job_id, data_reduction_id=data_reduction_id
            )
        )

    def insert_data_reduction(self, conn: Connection, r: DBDataReduction) -> int:
        result = conn.execute(
            sa.insert(self.tables.reductions).values(
                crystal_id=r.crystal_id,
                run_id=r.run_id,
                analysis_time=r.analysis_time,
                folder_path=str(r.folder_path),
                mtz_path=str(r.mtz_path) if r.mtz_path is not None else None,
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
        tools = self.tables.tools
        return [
            DBTool(
                id=row["id"],
                created=row["created"],
                name=row["name"],
                executable_path=Path(row["executable_path"]),
                extra_files=row["extra_files"],
                command_line=row["command_line"],
                description=row["description"],
                inputs=_convert_command_line(parse_command_line(row["command_line"])),
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
        conn.execute(
            sa.delete(self.tables.tools).where(self.tables.tools.c.id == tool_id)
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
        # noinspection PyComparisonWithNone
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

    def retrieve_crystals(self, conn: Connection) -> List[DBCrystal]:
        return [
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
        ]

    def retrieve_sample_data(self, conn: Connection) -> DBSampleData:
        return DBSampleData(
            pucks=self.retrieve_pucks(conn),
            crystals=self.retrieve_crystals(conn),
        )

    def retrieve_analysis_diffractions(
        self, conn: Connection, filter_query: str
    ) -> List[Tuple[str, int]]:
        _all_columns, query = self._analysis_filter_query(
            filter_query, sort_column=None, sort_order_desc=False
        )

        return [
            (row["diff_crystal_id"], row["diff_run_id"])
            for row in conn.execute(query)
            if row["diff_crystal_id"] is not None
        ]

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
            if isinstance(v, Beamline):
                return v.value
            if isinstance(v, DiffractionType):
                return v.value
            if isinstance(v, ReductionMethod):
                return v.value
            return v

        results: List[DBAnalysisRow] = []
        number_of_results = 0
        diffractions: Set[Tuple[str, int]] = set()
        for row in conn.execute(query):
            number_of_results += 1
            if limit is None or len(results) < limit:
                results.append(row)
            diffractions.add((row["diff_crystal_id"], row["diff_run_id"]))

        return DBAnalysisResult(
            columns=[c.name for c in all_columns],
            rows=[
                [postprocess(value) for _, value in row.items()]
                for row in conn.execute(query)
            ],
            total_diffractions=len(diffractions),
            total_rows=number_of_results,
        )

    def _analysis_filter_query(
        self, filter_query, sort_column, sort_order_desc
    ) -> Tuple[List[Label], Select]:
        crystal_columns = [
            c.label("crystals_" + c.name)
            for c in self.tables.crystals.c
            # self.tables.crystals.c.crystal_id.label("crystals_crystal_id"),
            # self.tables.crystals.c.created.label("crystals_created"),
        ]
        diffraction_columns = [c.label("diff_" + c.name) for c in self.tables.diffs.c]
        reduction_columns = [c.label("dr_" + c.name) for c in self.tables.reductions.c]
        all_columns = (
            crystal_columns
            + diffraction_columns
            + reduction_columns
            + [
                self.tables.jobs.c.id.label("jobs_id"),
                self.tables.jobs.c.tool_inputs.label("jobs_tool_inputs"),
                self.tables.tools.c.name.label("tools_name"),
            ]
        )
        query = (
            sa.select(all_columns)
            .select_from(
                self.tables.crystals.outerjoin(self.tables.diffs)
                .outerjoin(
                    self.tables.reductions,
                    onclause=sa.and_(
                        self.tables.reductions.c.run_id == self.tables.diffs.c.run_id,
                        self.tables.diffs.c.crystal_id
                        == self.tables.reductions.c.crystal_id,
                    ),
                )
                .outerjoin(self.tables.job_reductions)
                .outerjoin(self.tables.jobs)
                .outerjoin(self.tables.tools)
            )
            .where(sa.text(filter_query))
        )
        if sort_column is not None:
            query = query.order_by(
                sa.desc(sort_column) if sort_order_desc else sort_column
            )
        return all_columns, query

    def insert_job(self, conn: Connection, job: DBJob) -> int:
        result = conn.execute(
            sa.insert(self.tables.jobs).values(
                {
                    "queued": job.queued,
                    "started": job.started,
                    "stopped": job.stopped,
                    "status": job.status,
                    "failure_reason": job.failure_reason,
                    "output_directory": job.output_directory,
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
        conn.execute(
            sa.insert(self.tables.reduction_jobs).values(
                run_id=run_id, crystal_id=crystal_id, job_id=job_id
            )
        )
