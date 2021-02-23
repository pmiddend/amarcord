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
from amarcord.modules.spb.tables import Tables, run_property_atomic_db_columns
from amarcord.modules.spb.run_property import RunProperty
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.dbcontext import DBContext
from amarcord.util import dict_union, remove_duplicates_stable


@dataclass(frozen=True)
class RunSimple:
    run_id: RunId
    finished: bool


@dataclass(frozen=True)
class Comment:
    id: Optional[int]
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

        interesting_columns = run_property_atomic_db_columns(self._tables)
        select_stmt = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    tag.c.tag_text,
                ]
                + list(interesting_columns.values())
            )
            .select_from(run.outerjoin(tag).outerjoin(comment))
            .where(run.c.proposal_id == proposal_id)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[Dict[RunProperty, Any]] = []
        for _run_id, run_rows in groupby(
            conn.execute(select_stmt).fetchall(),
            lambda x: x[interesting_columns[RunProperty.RUN_ID].name],
        ):
            rows = list(run_rows)
            first_row = rows[0]
            tags = set(row["tag_text"] for row in rows if row["tag_text"] is not None)
            comments = remove_duplicates_stable(
                Comment(
                    id=row["comment_id"],
                    author=row["author"],
                    text=row["comment_text"],
                    created=row["created"],
                )
                for row in rows
                if row[0] is not None
            )
            result.append(
                dict_union(
                    {
                        RunProperty.TAGS: tags,
                        RunProperty.COMMENTS: comments,
                    },
                    {k: first_row[v.name] for k, v in interesting_columns.items()},
                )
            )
        return result

    def retrieve_run_ids(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[RunSimple]:
        return [
            RunSimple(row["id"], row["status"] == "finished")
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
        assert c.id is not None

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
        run = self._tables.run
        run_c = run.c
        comment = self._tables.run_comment
        tag = self._tables.run_tag
        interesting_columns = run_property_atomic_db_columns(self._tables)
        select_statement = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    tag.c.tag_text,
                    run_c.karabo,
                ]
                + list(interesting_columns.values())
            )
            .select_from(run.outerjoin(tag).outerjoin(comment))
            .where(run_c.id == run_id)
        )
        run_rows = conn.execute(select_statement).fetchall()
        if not run_rows:
            raise Exception(f"couldn't find any runs with id {run_id}")
        run_meta = run_rows[0]
        return Run(
            properties=dict_union(
                {
                    RunProperty.TAGS: remove_duplicates_stable(
                        [
                            row["tag_text"]
                            for row in run_rows
                            if row["tag_text"] is not None
                        ]
                    ),
                    RunProperty.COMMENTS: remove_duplicates_stable(
                        Comment(
                            row["comment_id"],
                            row["author"],
                            row["comment_text"],
                            row["created"],
                        )
                        for row in run_rows
                        if row["comment_id"] is not None
                    ),
                },
                {k: run_meta[v.name] for k, v in interesting_columns.items()},
            ),
            karabo=pickle.loads(run_meta["karabo"])
            if run_meta["karabo"] is not None
            else None,
        )

    def add_comment(self, conn: Connection, run_id: int, author: str, text: str) -> int:
        result = conn.execute(
            sa.insert(self._tables.run_comment).values(
                run_id=run_id,
                author=author,
                comment_text=text,
                created=datetime.datetime.utcnow(),
            )
        )
        return result.inserted_primary_key[0]

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
                    status="running",
                    started=datetime.datetime.utcnow(),
                    modified=datetime.datetime.utcnow(),
                )
            )
            return True

    def create_proposal(self, conn: Connection, prop_id: ProposalId) -> None:
        conn.execute(self._tables.proposal.insert().values(id=prop_id))

    def update_run_karabo(self, conn: Connection, run_id: RunId, karabo: bytes) -> None:
        conn.execute(
            sa.update(self._tables.run)
            .where(self._tables.run.c.id == run_id)
            .values(karabo=karabo)
        )

    def update_run_property(
        self, conn: Connection, run_id: int, runprop: RunProperty, value: Any
    ) -> None:
        if runprop == RunProperty.TAGS:
            if not isinstance(value, list):
                raise ValueError(f"tags should be a list, got {type(value)}")
            self.change_tags(conn, run_id, value)
            return
        db_columns = run_property_atomic_db_columns(self._tables)
        assert runprop in db_columns
        conn.execute(
            sa.update(self._tables.run)
            .where(self._tables.run.c.id == run_id)
            .values({db_columns[runprop]: value})
        )

    def connect(self) -> Connection:
        return self.dbcontext.connect()
