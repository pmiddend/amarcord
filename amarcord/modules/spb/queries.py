from dataclasses import dataclass
from typing import List
from typing import Dict
from typing import Optional
from typing import Any
from itertools import groupby
import datetime
import sqlalchemy as sa
from amarcord.modules.spb.tables import Tables
from amarcord.modules.spb.column import Column
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.util import remove_duplicates


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
    sample_id: int
    tags: List[str]
    status: str
    repetition_rate_mhz: float
    comments: List[Comment]


def retrieve_runs(
    tables: Tables, conn: Any, proposal_id: ProposalId
) -> List[Dict[Column, Any]]:
    run = tables.run
    tag = tables.run_tag

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
            ]
        )
        .select_from(run.outerjoin(tag))
        .where(run.c.proposal_id == proposal_id)
        .order_by(run.c.id)
    )
    result: List[Dict[Column, Any]] = []
    for _run_id, run_rows in groupby(
        conn.execute(select_stmt).fetchall(), lambda x: x[0]
    ):
        rows = list(run_rows)
        first_row = rows[0]
        result.append(
            {
                Column.RUN_ID: first_row[0],
                Column.STATUS: first_row[1],
                Column.SAMPLE: first_row[2],
                Column.REPETITION_RATE: first_row[3],
                Column.PULSE_ENERGY: first_row[4],
                Column.TAGS: set(row[5] for row in rows if row[5] is not None),
                Column.STARTED: first_row[6],
            }
        )
    return result


def retrieve_run_ids(
    conn: Any, tables: Tables, proposal_id: ProposalId
) -> List[RunSimple]:
    return [
        RunSimple(row[0], row[1] == "finished")
        for row in conn.execute(
            sa.select([tables.run.c.id, tables.run.c.status])
            .where(tables.run.c.proposal_id == proposal_id)
            .order_by(tables.run.c.id)
        ).fetchall()
    ]


def retrieve_tags(conn: Any, tables: Tables) -> List[str]:
    return [
        row[0]
        for row in conn.execute(
            sa.select([tables.run_tag.c.tag_text])
            .order_by(tables.run_tag.c.tag_text)
            .distinct()
        ).fetchall()
    ]


def retrieve_sample_ids(conn: Any, tables: Tables) -> List[int]:
    return [
        row[0]
        for row in conn.execute(
            sa.select([tables.sample.c.sample_id]).order_by(tables.sample.c.sample_id)
        ).fetchall()
    ]


def change_sample(
    conn: Any, tables: Tables, run_id: int, new_sample: Optional[int]
) -> None:
    conn.execute(
        sa.update(tables.run)
        .where(tables.run.c.id == run_id)
        .values(sample_id=new_sample)
    )


def change_comment(conn: Any, tables: Tables, c: Comment) -> None:
    conn.execute(
        sa.update(tables.run_comment)
        .where(tables.run_comment.c.id == c.id)
        .values(author=c.author, text=c.text)
    )


def change_tags(conn: Any, tables: Tables, run_id: int, new_tags: List[str]) -> None:
    with conn.begin():
        conn.execute(sa.delete(tables.run_tag).where(tables.run_tag.c.run_id == run_id))
        if new_tags:
            conn.execute(
                tables.run_tag.insert(),
                [{"run_id": run_id, "tag_text": t} for t in new_tags],
            )


def retrieve_run(conn: Any, tables: Tables, run_id: int) -> Run:
    run_c = tables.run.c
    select_statement = (
        sa.select(
            [
                run_c.id,
                run_c.sample_id,
                run_c.status,
                run_c.repetition_rate_mhz,
                tables.run_tag.c.tag_text,
                tables.run_comment.c.id.label("comment_id"),
                tables.run_comment.c.author,
                tables.run_comment.c.text,
                tables.run_comment.c.created,
            ]
        )
        .select_from(tables.run.outerjoin(tables.run_tag).outerjoin(tables.run_comment))
        .where(run_c.id == run_id)
    )
    run_rows = conn.execute(select_statement).fetchall()
    if not run_rows:
        raise Exception(f"couldn't find any runs with id {run_id}")
    run_meta = run_rows[0]
    return Run(
        sample_id=run_meta["sample_id"],
        status=run_meta["status"],
        repetition_rate_mhz=run_meta["repetition_rate_mhz"],
        tags=remove_duplicates(
            [row["tag_text"] for row in run_rows if row["tag_text"] is not None]
        ),
        comments=remove_duplicates(
            Comment(row["comment_id"], row["author"], row["text"], row["created"])
            for row in run_rows
            if row["author"] is not None
        ),
    )


def add_comment(conn: Any, tables: Tables, run_id: int, author: str, text: str) -> None:
    conn.execute(
        sa.insert(tables.run_comment).values(
            run_id=run_id, author=author, text=text, created=datetime.datetime.utcnow()
        )
    )


def delete_comment(conn: Any, tables: Tables, comment_id: int) -> None:
    conn.execute(
        sa.delete(tables.run_comment).where(tables.run_comment.c.id == comment_id)
    )


def create_run(
    tables: Tables,
    conn: Any,
    proposal_id: ProposalId,
    run_id: int,
    sample_id: Optional[int],
) -> bool:
    with conn.begin():
        run_exists = conn.execute(
            sa.select([tables.run.c.id]).where(tables.run.c.id == run_id)
        ).fetchall()
        if len(run_exists) > 0:
            return False

        conn.execute(
            tables.run.insert().values(
                proposal_id=proposal_id,
                id=run_id,
                sample_id=sample_id,
                status="started",
                started=datetime.datetime.utcnow(),
                modified=datetime.datetime.utcnow(),
            )
        )
        return True
