from dataclasses import dataclass
from typing import List
from typing import Dict
from typing import Optional
from typing import Any
from typing import Tuple
from itertools import groupby
import pickle
import datetime
import sqlalchemy as sa
from amarcord.modules.spb.tables import Tables
from amarcord.modules.spb.column import RunProperty
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.dbcontext import DBContext
from amarcord.util import remove_duplicates_stable


@dataclass(frozen=True)
class RunSimple:
    run_id: RunId
    finished: bool


@dataclass(frozen=True)
class Comment:
    id: int
    author: str
    text: str
    created: datetime.datetime


@dataclass(frozen=True)
class Run:
    properties: Dict[RunProperty, Any]
    karabo: Tuple[Dict[str, Any], Dict[str, Any]]


Connection = Any


class SPBQueries:
    def __init__(self, dbcontext: DBContext, tables: Tables) -> None:
        self.dbcontext = dbcontext
        self._tables = tables

    def retrieve_runs(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[Dict[RunProperty, Any]]:
        run = self._tables.run
        tag = self._tables.run_tag
        comment = self._tables.run_comment

        select_stmt = (
            sa.select(
                [
                    run.c.id,
                    run.c.status,
                    run.c.sample_id,
                    run.c.repetition_rate_mhz,
                    run.c.pulse_energy_mj,
                    tag.c.tag_text,
                    run.c.started,
                    run.c.hit_rate,
                    run.c.indexing_rate,
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    run.c.xray_energy_kev,
                    run.c.injector_position_z_mm,
                    run.c.detector_distance_mm,
                    run.c.injector_flow_rate,
                    run.c.trains,
                    run.c.sample_delivery_rate,
                ]
            )
            .select_from(run.outerjoin(tag).outerjoin(comment))
            .where(run.c.proposal_id == proposal_id)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[Dict[RunProperty, Any]] = []
        for _run_id, run_rows in groupby(
            conn.execute(select_stmt).fetchall(), lambda x: x[0]
        ):
            rows = list(run_rows)
            first_row = rows[0]
            tags = set(row[5] for row in rows if row[5] is not None)
            comments = remove_duplicates_stable(
                Comment(row[9], row[10], row[11], row[12])
                for row in rows
                if row[9] is not None
            )
            result.append(
                {
                    RunProperty.RUN_ID: first_row[0],
                    RunProperty.STATUS: first_row[1],
                    RunProperty.SAMPLE: first_row[2],
                    RunProperty.REPETITION_RATE: first_row[3],
                    RunProperty.PULSE_ENERGY: first_row[4],
                    RunProperty.TAGS: tags,
                    RunProperty.STARTED: first_row[6],
                    RunProperty.HIT_RATE: first_row[7],
                    RunProperty.INDEXING_RATE: first_row[8],
                    RunProperty.X_RAY_ENERGY: first_row[13],
                    RunProperty.INJECTOR_POSITION_Z_MM: first_row[14],
                    RunProperty.DETECTOR_DISTANCE_MM: first_row[15],
                    RunProperty.INJECTOR_FLOW_RATE: first_row[16],
                    RunProperty.TRAINS: first_row[17],
                    RunProperty.SAMPLE_DELIVERY_RATE: first_row[18],
                    RunProperty.COMMENTS: comments,
                }
            )
        return result

    def retrieve_run_ids(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[RunSimple]:
        return [
            RunSimple(row[0], row[1] == "finished")
            for row in conn.execute(
                sa.select([self._tables.run.c.id, self._tables.run.c.status])
                .where(self._tables.run.c.proposal_id == proposal_id)
                .order_by(self._tables.run.c.id)
            ).fetchall()
        ]

    def retrieve_tags(self, conn: Connection) -> List[str]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self._tables.run_tag.c.tag_text])
                .order_by(self._tables.run_tag.c.tag_text)
                .distinct()
            ).fetchall()
        ]

    def retrieve_sample_ids(self, conn: Connection) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self._tables.sample.c.sample_id]).order_by(
                    self._tables.sample.c.sample_id
                )
            ).fetchall()
        ]

    def change_sample(
        self, conn: Connection, run_id: int, new_sample: Optional[int]
    ) -> None:
        conn.execute(
            sa.update(self._tables.run)
            .where(self._tables.run.c.id == run_id)
            .values(sample_id=new_sample)
        )

    def change_comment(self, conn: Connection, c: Comment) -> None:
        conn.execute(
            sa.update(self._tables.run_comment)
            .where(self._tables.run_comment.c.id == c.id)
            .values(author=c.author, comment_text=c.text)
        )

    def change_tags(self, conn: Connection, run_id: int, new_tags: List[str]) -> None:
        with conn.begin():
            conn.execute(
                sa.delete(self._tables.run_tag).where(
                    self._tables.run_tag.c.run_id == run_id
                )
            )
            if new_tags:
                conn.execute(
                    self._tables.run_tag.insert(),
                    [{"run_id": run_id, "tag_text": t} for t in new_tags],
                )

    def retrieve_run(self, conn: Connection, run_id: int) -> Run:
        run_c = self._tables.run.c
        select_statement = (
            sa.select(
                [
                    run_c.id,
                    run_c.sample_id,
                    run_c.status,
                    run_c.repetition_rate_mhz,
                    self._tables.run_tag.c.tag_text,
                    self._tables.run_comment.c.id.label("comment_id"),
                    self._tables.run_comment.c.author,
                    self._tables.run_comment.c.comment_text,
                    self._tables.run_comment.c.created,
                    run_c.karabo,
                ]
            )
            .select_from(
                self._tables.run.outerjoin(self._tables.run_tag).outerjoin(
                    self._tables.run_comment
                )
            )
            .where(run_c.id == run_id)
        )
        run_rows = conn.execute(select_statement).fetchall()
        if not run_rows:
            raise Exception(f"couldn't find any runs with id {run_id}")
        run_meta = run_rows[0]
        return Run(
            properties={
                RunProperty.SAMPLE: run_meta["sample_id"],
                RunProperty.STATUS: run_meta["status"],
                RunProperty.REPETITION_RATE: run_meta["repetition_rate_mhz"],
                RunProperty.TAGS: remove_duplicates_stable(
                    [row["tag_text"] for row in run_rows if row["tag_text"] is not None]
                ),
                RunProperty.COMMENTS: remove_duplicates_stable(
                    Comment(
                        row["comment_id"],
                        row["author"],
                        row["comment_text"],
                        row["created"],
                    )
                    for row in run_rows
                    if row["author"] is not None
                ),
            },
            karabo=pickle.loads(run_meta["karabo"])
            if run_meta["karabo"] is not None
            else None,
        )

    def add_comment(
        self, conn: Connection, run_id: int, author: str, text: str
    ) -> None:
        conn.execute(
            sa.insert(self._tables.run_comment).values(
                run_id=run_id,
                author=author,
                comment_text=text,
                created=datetime.datetime.utcnow(),
            )
        )

    def delete_comment(self, conn: Connection, comment_id: int) -> None:
        conn.execute(
            sa.delete(self._tables.run_comment).where(
                self._tables.run_comment.c.id == comment_id
            )
        )

    def create_run(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        run_id: RunId,
        sample_id: Optional[int],
    ) -> bool:
        with conn.begin():
            run_exists = conn.execute(
                sa.select([self._tables.run.c.id]).where(
                    self._tables.run.c.id == run_id
                )
            ).fetchall()
            if len(run_exists) > 0:
                return False

            conn.execute(
                self._tables.run.insert().values(
                    proposal_id=proposal_id,
                    id=run_id,
                    sample_id=sample_id,
                    status="started",
                    started=datetime.datetime.utcnow(),
                    modified=datetime.datetime.utcnow(),
                )
            )
            return True

    def create_proposal(self, conn: Connection, prop_id: ProposalId) -> None:
        conn.execute(self._tables.proposal.insert().values(id=prop_id))

    def update_run(self, conn: Connection, run_id: RunId, karabo: bytes) -> None:
        conn.execute(
            sa.update(self._tables.run)
            .where(self._tables.run.c.id == run_id)
            .values(karabo=karabo)
        )
