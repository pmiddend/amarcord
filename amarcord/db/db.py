import datetime
import logging
import pickle
import re
from itertools import groupby
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Final
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union
from typing import cast

import bcrypt
import sqlalchemy as sa
from sqlalchemy import and_

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_json_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.comment import DBComment
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.constants import DB_SOURCE_NAME
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.indexing_job_status import IndexingJobStatus
from amarcord.db.karabo import Karabo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBAugmentedIndexingParameter
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBIndexingJob
from amarcord.db.table_classes import DBIndexingParameter
from amarcord.db.table_classes import DBIndexingRunData
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBRunWithIndexingResult
from amarcord.db.table_classes import DBSample
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import DBContext
from amarcord.query_parser import Row as QueryRow
from amarcord.util import dict_union
from amarcord.util import remove_duplicates_stable

logger = logging.getLogger(__name__)

VALIDATE_ATTRIBUTI: Final = True


OverviewAttributi = Dict[AssociatedTable, AttributiMap]


class RunNotFound(Exception):
    pass


def _run_to_attributi(r: DBRun, types: Dict[AttributoId, DBAttributo]) -> AttributiMap:
    result = AttributiMap(types, r.attributi)
    result.append_to_source(
        DB_SOURCE_NAME,
        {
            AttributoId("id"): r.id,
            AttributoId("sample_id"): r.sample_id,
            AttributoId("modified"): r.modified,
            AttributoId("comments"): r.comments,
            AttributoId("proposal_id"): r.proposal_id,
        },
    )
    return result


def _sample_to_attributi(
    s: DBSample, types: Dict[AttributoId, DBAttributo]
) -> AttributiMap:
    result = AttributiMap(types, s.attributi)
    result.append_to_source(DB_SOURCE_NAME, {AttributoId("id"): s.id})
    result.append_to_source(DB_SOURCE_NAME, {AttributoId("name"): s.name})
    return result


def _convert_job(id_column: str, r) -> DBIndexingJob:
    return DBIndexingJob(
        id=r[id_column],
        started=r["started"],
        stopped=r["stopped"],
        run_id=r["run_id"],
        output_directory=r["output_directory"],
        indexing_parameter_id=r["indexing_parameter_id"],
        master_file=r["master_file"],
        command_line=r["command_line"],
        status=r["status"],
        slurm_job_id=r["slurm_job_id"],
        error_message=r["error_message"],
        result_file=r["result_file"],
    )


def validate_attributi(
    metadata: Dict[AttributoId, DBAttributo], attributi: RawAttributiMap
) -> None:
    AttributiMap(metadata, attributi)


class DB:
    def __init__(self, dbcontext: DBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def overview_update_time(self, conn: Connection) -> Optional[datetime.datetime]:
        max_run, max_sample = conn.execute(
            sa.select(
                [
                    sa.func.max(self.tables.run.c.modified),
                    sa.func.max(self.tables.sample.c.modified),
                ]
            ).select_from(self.tables.run.outerjoin(self.tables.sample))
        ).fetchone()
        # We are compromising here: if there is either no run (yet) or no samples (yet), we return None
        # which downstream should handle as "list
        # needs update!"
        return (
            max(max_run, max_sample)
            if max_run is not None and max_sample is not None
            else None
        )

    def run_count(self, conn: Connection) -> int:
        return conn.execute(
            sa.select([sa.func.count()]).select_from(self.tables.run)
        ).fetchone()[0]

    def retrieve_overview(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        types: Dict[AssociatedTable, Dict[AttributoId, DBAttributo]],
    ) -> List[OverviewAttributi]:
        sample_types = types[AssociatedTable.SAMPLE]
        samples: Dict[int, AttributiMap] = {
            cast(int, k.id): _sample_to_attributi(k, sample_types)
            for k in self.retrieve_samples(conn, proposal_id)
        }
        runs: List[DBRun] = self.retrieve_runs(conn, proposal_id, None)

        run_types = types[AssociatedTable.RUN]
        result: List[OverviewAttributi] = []
        for r in runs:
            sample_id = r.sample_id
            sample = samples.get(sample_id, None) if sample_id is not None else None
            result.append(
                {
                    AssociatedTable.SAMPLE: sample
                    if sample is not None
                    else AttributiMap(sample_types, None),
                    AssociatedTable.RUN: _run_to_attributi(r, run_types),
                }
            )
        return result

    def retrieve_runs(
        self,
        conn: Connection,
        proposal_id: Optional[ProposalId],
        since: Optional[datetime.datetime],
    ) -> List[DBRun]:
        run = self.tables.run
        comment = self.tables.run_comment

        where_condition = (
            run.c.proposal_id == proposal_id if proposal_id is not None else True
        )
        if since:
            # mypy complains: error: Value of type variable "_CLE" of function cannot be "object"
            # not sure what that means
            where_condition = and_(where_condition, run.c.modified >= since)  # type: ignore
        select_stmt = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    run.c.id,
                    run.c.sample_id,
                    run.c.proposal_id,
                    run.c.modified,
                    run.c.attributi,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(where_condition)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[DBRun] = []
        # before = time()
        select_results = conn.execute(select_stmt).fetchall()
        # after = time()
        # logger.info("Retrieved runs in %ss", after - before)
        for _run_id, run_rows in groupby(
            select_results,
            lambda x: x["id"],
        ):
            rows = list(run_rows)
            run_meta = rows[0]
            comments = remove_duplicates_stable(
                DBComment(
                    id=row["comment_id"],
                    run_id=run_meta["id"],
                    author=row["author"],
                    text=row["comment_text"],
                    created=row["created"],
                )
                for row in rows
                if row[0] is not None
            )[0:5]
            result.append(
                DBRun(
                    RawAttributiMap(run_meta["attributi"]),
                    run_meta["id"],
                    run_meta["sample_id"],
                    run_meta["proposal_id"],
                    run_meta["modified"],
                    comments,
                )
            )
        return result

    def retrieve_run_ids(self, conn: Connection, proposal_id: ProposalId) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.run.c.id])
                .where(self.tables.run.c.proposal_id == proposal_id)
                .order_by(self.tables.run.c.id)
            ).fetchall()
        ]

    def retrieve_mini_samples(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[DBMiniSample]:
        return [
            DBMiniSample(row[0], row[1])
            for row in conn.execute(
                sa.select([self.tables.sample.c.id, self.tables.sample.c.name])
                .order_by(self.tables.sample.c.name)
                .where(self.tables.sample.c.proposal_id == proposal_id)
            ).fetchall()
        ]

    def change_comment(self, conn: Connection, c: DBComment) -> None:
        assert c.id is not None

        if not c.text.strip():
            raise ValueError("Text (after white-space stripping) is empty")
        if not c.author.strip():
            raise ValueError("Author (after white-space stripping) is empty")

        conn.execute(
            sa.update(self.tables.run_comment)
            .where(self.tables.run_comment.c.id == c.id)
            .values(author=c.author.strip(), comment_text=c.text.strip())
        )
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == c.run_id)
            .values(modified=datetime.datetime.utcnow())
        )

    def retrieve_run(
        self, conn: Connection, proposal_id: ProposalId, run_id: int
    ) -> DBRun:
        run = self.tables.run
        run_c = run.c
        comment = self.tables.run_comment
        select_statement = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    run.c.id,
                    run.c.sample_id,
                    run.c.proposal_id,
                    run.c.modified,
                    run.c.attributi,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(and_(run_c.id == run_id, run_c.proposal_id == proposal_id))
        )
        run_rows = conn.execute(select_statement).fetchall()
        if not run_rows:
            raise RunNotFound()
        run_meta = run_rows[0]
        return DBRun(
            RawAttributiMap(run_meta["attributi"]),
            run_meta["id"],
            run_meta["sample_id"],
            run_meta["proposal_id"],
            run_meta["modified"],
            remove_duplicates_stable(
                DBComment(
                    row["comment_id"],
                    run_meta["id"],
                    row["author"],
                    row["comment_text"],
                    row["created"],
                )
                for row in run_rows
                if row["comment_id"] is not None
            ),
        )

    def add_comment(
        self,
        conn: Connection,
        run_id: int,
        author: str,
        text: str,
        time: Optional[str] = None,
    ) -> int:
        if not text.strip():
            raise ValueError("Text (after white-space stripping) is empty")
        if not author.strip():
            raise ValueError("Author (after white-space stripping) is empty")
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(modified=datetime.datetime.utcnow())
        )
        result = conn.execute(
            sa.insert(self.tables.run_comment).values(
                run_id=run_id,
                author=author.strip(),
                comment_text=text.strip(),
                created=datetime.datetime.utcnow() if time is None else time,
            )
        )
        return result.inserted_primary_key[0]

    def delete_comment(self, conn: Connection, run_id: int, comment_id: int) -> None:
        conn.execute(
            sa.delete(self.tables.run_comment).where(
                self.tables.run_comment.c.id == comment_id
            )
        )
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(modified=datetime.datetime.utcnow())
        )

    def add_run(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        run_id: int,
        sample_id: Optional[int],
        attributi: RawAttributiMap,
    ) -> bool:
        with conn.begin():
            if VALIDATE_ATTRIBUTI:
                validate_attributi(
                    self.retrieve_table_attributi(conn, AssociatedTable.RUN), attributi
                )
            run_exists = conn.execute(
                sa.select([self.tables.run.c.id]).where(self.tables.run.c.id == run_id)
            ).fetchall()
            if len(run_exists) > 0:
                return False

            conn.execute(
                self.tables.run.insert().values(
                    proposal_id=proposal_id,
                    id=run_id,
                    sample_id=sample_id,
                    attributi=attributi.to_json(),
                    modified=datetime.datetime.utcnow(),
                )
            )
            return True

    def add_proposal(
        self,
        conn: Connection,
        prop_id: ProposalId,
        admin_password_plaintext: Optional[str] = None,
    ) -> None:
        hashed_password: Optional[str] = None
        if admin_password_plaintext is not None:
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(
                admin_password_plaintext.encode("utf-8"), salt
            ).decode("utf-8")
        conn.execute(
            self.tables.proposal.insert().values(
                id=prop_id, admin_password=hashed_password
            )
        )

    def change_proposal_password(
        self,
        conn: Connection,
        prop_id: ProposalId,
        admin_password_plaintext: Optional[str],
    ) -> None:
        hashed_password: Optional[str] = None
        if admin_password_plaintext:
            salt = bcrypt.gensalt()
            hashed_password = bcrypt.hashpw(
                admin_password_plaintext.encode("utf-8"), salt
            ).decode("utf-8")
        conn.execute(
            self.tables.proposal.update()
            .values(admin_password=hashed_password)
            .where(self.tables.proposal.c.id == prop_id)
        )

    def proposal_has_password(self, conn: Connection, prop_id: ProposalId) -> bool:
        return (
            conn.execute(
                sa.select([self.tables.proposal.c.admin_password]).where(
                    self.tables.proposal.c.id == prop_id
                )
            ).one()[0]
            is not None
        )

    def check_proposal_password(
        self, conn: Connection, prop_id: ProposalId, admin_password_plaintext: str
    ) -> bool:
        password = conn.execute(
            sa.select([self.tables.proposal.c.admin_password]).where(
                self.tables.proposal.c.id == prop_id
            )
        ).fetchone()[0]

        if password is None:
            return True

        return bcrypt.checkpw(
            admin_password_plaintext.encode("utf-8"), password.encode("utf-8")
        )

    def retrieve_attributi(
        self,
        conn: Connection,
        filter_table: Optional[AssociatedTable] = None,
        inherent: bool = True,
    ) -> Dict[AssociatedTable, Dict[AttributoId, DBAttributo]]:
        select_stmt = sa.select(
            [
                self.tables.attributo.c.name,
                self.tables.attributo.c.description,
                self.tables.attributo.c.json_schema,
                self.tables.attributo.c.associated_table,
            ]
        ).order_by(self.tables.attributo.c.associated_table)

        if filter_table is not None:
            select_stmt = select_stmt.where(
                self.tables.attributo.c.associated_table == filter_table
            )

        result = {
            table: {
                AttributoId(row[0]): DBAttributo(
                    name=AttributoId(row[0]),
                    description=row[1],
                    attributo_type=schema_json_to_attributo_type(json_schema=row[2]),
                    associated_table=table,
                )
                for row in rows
            }
            for table, rows in groupby(
                conn.execute(select_stmt).fetchall(),
                lambda x: x[3],
            )
        }
        if inherent:
            for table, attributi in self.tables.additional_attributi.items():
                if table not in result:
                    result[table] = attributi
                else:
                    result[table].update(attributi)
        return result

    def retrieve_table_attributi(
        self, conn: Connection, table: AssociatedTable, inherent: bool = True
    ) -> Dict[AttributoId, DBAttributo]:
        return self.retrieve_attributi(conn, table, inherent).get(table, {})

    def run_attributi(self, conn: Connection) -> Dict[AttributoId, DBAttributo]:
        return self.retrieve_table_attributi(conn, AssociatedTable.RUN)

    def update_sample_attributo(
        self, conn: Connection, sample_id: int, attributo: AttributoId, value: Any
    ) -> None:
        current_json = conn.execute(
            sa.select([self.tables.sample.c.attributi]).where(
                self.tables.sample.c.id == sample_id
            )
        ).first()[0]
        assert current_json is None or isinstance(
            current_json, dict
        ), f"attributi should be None or dictionary, got {type(current_json)}"
        if current_json is None:
            current_json = {}
        attributi_map = RawAttributiMap(current_json)
        if value is not None:
            attributi_map.set_single_manual(attributo, value)
        else:
            attributi_map.remove_manual_attributo(attributo)
        self.update_sample_attributi(conn, sample_id, attributi_map)

    def update_run_attributi(
        self, conn: Connection, run_id: int, attributi: RawAttributiMap
    ) -> None:
        if VALIDATE_ATTRIBUTI:
            validate_attributi(
                self.retrieve_table_attributi(conn, AssociatedTable.RUN), attributi
            )
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(attributi=attributi.to_json(), modified=datetime.datetime.utcnow())
        )

    def update_sample_attributi(
        self, conn: Connection, sample_id: int, attributi: RawAttributiMap
    ) -> None:
        if VALIDATE_ATTRIBUTI:
            validate_attributi(
                self.retrieve_table_attributi(conn, AssociatedTable.SAMPLE), attributi
            )
        conn.execute(
            sa.update(self.tables.sample)
            .where(self.tables.sample.c.id == sample_id)
            .values(attributi=attributi.to_json(), modified=datetime.datetime.utcnow())
        )

    def update_run_attributo(
        self,
        conn: Connection,
        run_id: int,
        attributo: AttributoId,
        value: AttributoValue,
        source: str = MANUAL_SOURCE_NAME,
    ) -> None:
        if attributo == AttributoId("sample_id"):
            assert value is None or isinstance(value, int)
            conn.execute(
                sa.update(self.tables.run)
                .where(self.tables.run.c.id == run_id)
                .values(sample_id=value, modified=datetime.datetime.utcnow())
            )
            return

        current_json = conn.execute(
            sa.select([self.tables.run.c.attributi]).where(
                self.tables.run.c.id == run_id
            )
        ).first()[0]
        assert current_json is None or isinstance(
            current_json, dict
        ), f"attributi should be None or dictionary, got {type(current_json)}"
        if current_json is None:
            current_json = {}
        attributi_map = RawAttributiMap(current_json)
        if value is not None:
            attributi_map.append_single_to_source(source, attributo, value)
        else:
            attributi_map.remove_attributo(attributo, source)
        self.update_run_attributi(conn, run_id, attributi_map)

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    # pylint: disable=no-self-use
    # noinspection PyMethodMayBeStatic
    def retrieve_karabo(self, _conn: Connection, _run_id: int) -> Optional[Karabo]:
        result: Optional[bytes]
        # noinspection PyBroadException
        try:
            with open("data/pickled_karabo", "rb") as f:
                result = f.read()
        except:
            result = None
        return pickle.loads(result) if result is not None else None

    def add_attributo(
        self,
        conn: Connection,
        name: str,
        description: str,
        associated_table: AssociatedTable,
        prop_type: AttributoType,
    ) -> None:
        if not re.fullmatch(ATTRIBUTO_NAME_REGEX, name, re.IGNORECASE):
            raise ValueError(
                f'attributo name "{name}" contains invalid characters (maybe a number at the beginning '
                f"or a dash?)"
            )
        conn.execute(
            self.tables.attributo.insert().values(
                name=name,
                description=description,
                associated_table=associated_table,
                json_schema=attributo_type_to_schema(prop_type),
            )
        )

    def retrieve_used_sample_ids(self, conn: Connection) -> Dict[int, List[int]]:
        result: Dict[int, List[int]] = {}
        db_results = conn.execute(
            sa.select(
                [
                    self.tables.sample.c.id,
                    self.tables.run.c.id,
                ]
            )
            .order_by(self.tables.sample.c.id)
            .select_from(self.tables.sample.join(self.tables.run))
        ).fetchall()
        for sample_id, run_ids in groupby(
            db_results,
            lambda x: x[0],
        ):
            result[sample_id] = list(r[1] for r in run_ids)
        return result

    def retrieve_samples(
        self,
        conn: Connection,
        proposal_id: Optional[ProposalId],
        since: Optional[datetime.datetime] = None,
    ) -> List[DBSample]:
        tc = self.tables.sample.c
        select_stmt = (
            sa.select(
                [
                    tc.id,
                    tc.proposal_id,
                    tc.name,
                    tc.attributi,
                ]
            )
            .where(
                self.tables.sample.c.proposal_id == proposal_id
                if proposal_id is not None
                else True
            )
            .order_by(tc.id)
        )
        if since is not None:
            select_stmt = select_stmt.where(tc.modified >= since)

        def prepare_sample(row: Any) -> DBSample:
            return DBSample(
                id=row["id"],
                proposal_id=ProposalId(row["proposal_id"]),
                name=row["name"],
                attributi=RawAttributiMap(row["attributi"]),
            )

        return [prepare_sample(row) for row in conn.execute(select_stmt).fetchall()]

    def add_sample(self, conn: Connection, t: DBSample) -> int:
        if VALIDATE_ATTRIBUTI:
            validate_attributi(
                self.retrieve_table_attributi(conn, AssociatedTable.SAMPLE), t.attributi
            )
        return conn.execute(
            sa.insert(self.tables.sample).values(
                name=t.name,
                proposal_id=t.proposal_id,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
                modified=datetime.datetime.utcnow(),
            )
        ).inserted_primary_key[0]

    def edit_sample(self, conn: Connection, t: DBSample) -> None:
        assert t.id is not None
        if VALIDATE_ATTRIBUTI:
            validate_attributi(
                self.retrieve_table_attributi(conn, AssociatedTable.SAMPLE), t.attributi
            )
        conn.execute(
            sa.update(self.tables.sample)
            .values(
                name=t.name,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
                modified=datetime.datetime.utcnow(),
            )
            .where(self.tables.sample.c.id == t.id)
        )

    def delete_sample(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == tid)
        )

    def delete_run(self, conn: Connection, rid: int) -> None:
        conn.execute(sa.delete(self.tables.run).where(self.tables.run.c.id == rid))

    def delete_attributo(
        self, conn: Connection, table: AssociatedTable, name: AttributoId
    ) -> None:
        with conn.begin():
            conn.execute(
                sa.delete(self.tables.attributo).where(
                    and_(
                        self.tables.attributo.c.name == str(name),
                        self.tables.attributo.c.associated_table == table,
                    )
                )
            )
            if table == AssociatedTable.RUN:
                for run in self.retrieve_runs(conn, proposal_id=None, since=None):
                    existed = run.attributi.remove_attributo(name, source=None)
                    if existed:
                        self.update_run_attributi(
                            conn,
                            run.id,
                            run.attributi,
                        )
            elif table == AssociatedTable.SAMPLE:
                for sample in self.retrieve_samples(conn, proposal_id=None, since=None):
                    existed = sample.attributi.remove_attributo(name, source=None)
                    if existed:
                        self.update_sample_attributi(
                            conn,
                            cast(int, sample.id),
                            sample.attributi,
                        )
            else:
                raise Exception(
                    f"cannot delete attributo {name} of table {table} because that code doesn't exist yet"
                )

    def have_proposals(self, conn: Connection) -> bool:
        return conn.execute(sa.select([self.tables.proposal.c.id])).fetchall()

    def add_event(
        self,
        conn: Connection,
        level: EventLogLevel,
        source: str,
        text: str,
        created: Optional[datetime.datetime] = None,
    ) -> None:
        conn.execute(
            sa.insert(self.tables.event_log).values(
                level=level,
                source=source,
                text=text,
                created=created if created is not None else datetime.datetime.utcnow(),
            )
        )

    def retrieve_events(
        self, conn: Connection, since: Optional[Union[datetime.datetime, int]] = None
    ) -> List[DBEvent]:
        result: List[DBEvent] = []
        for row in conn.execute(
            sa.select(
                [
                    self.tables.event_log.c.id,
                    self.tables.event_log.c.created,
                    self.tables.event_log.c.level,
                    self.tables.event_log.c.source,
                    self.tables.event_log.c.text,
                ]
            )
            .order_by(self.tables.event_log.c.created)
            .where(
                True
                if since is None
                else self.tables.event_log.c.created >= since
                if isinstance(since, datetime.datetime)
                else self.tables.event_log.c.id > since
            )
        ).fetchall():
            result.append(
                DBEvent(
                    row["id"], row["created"], row["level"], row["source"], row["text"]
                )
            )
        return result

    def retrieve_indexing_parameter(
        self, conn: Connection, id_: int
    ) -> DBIndexingParameter:
        ip = self.tables.indexing_parameter.c

        r = conn.execute(
            sa.select(
                [
                    ip.id,
                    ip.project_file_first_discovery,
                    ip.project_file_last_discovery,
                    ip.project_file_path,
                    ip.project_file_hash,
                    ip.project_file_content,
                    ip.geometry_file_content,
                ]
            ).where(ip.id == id_)
        ).one()

        if r is None:
            raise Exception(f"couldn't find indexing parameter with id {id}")

        return DBIndexingParameter(
            id=r["id"],
            project_file_first_discovery=r["project_file_first_discovery"],
            project_file_last_discovery=r["project_file_last_discovery"],
            project_file_path=r["project_file_path"],
            project_file_hash=r["project_file_hash"],
            project_file_content=r["project_file_content"],
            geometry_file_content=r["geometry_file_content"],
        )

    def retrieve_indexing_parameters(
        self, conn: Connection
    ) -> Iterable[DBAugmentedIndexingParameter]:
        ijp = self.tables.indexing_parameter.c
        ij = self.tables.indexing_job.c

        def _convert_parameter(r) -> DBAugmentedIndexingParameter:
            return DBAugmentedIndexingParameter(
                indexing_parameter=DBIndexingParameter(
                    id=r["id"],
                    project_file_first_discovery=r["project_file_first_discovery"],
                    project_file_last_discovery=r["project_file_last_discovery"],
                    project_file_path=r["project_file_path"],
                    project_file_hash=r["project_file_hash"],
                    project_file_content=None,
                    geometry_file_content=None,
                ),
                number_of_jobs=r["number_of_jobs"],
            )

        return (
            _convert_parameter(r)
            for r in conn.execute(
                sa.select(
                    [
                        ijp.id,
                        ijp.project_file_first_discovery,
                        ijp.project_file_last_discovery,
                        ijp.project_file_path,
                        ijp.project_file_hash,
                        sa.func.count(ij.id).label("number_of_jobs"),
                    ]
                )
                .outerjoin(self.tables.indexing_job, ij.indexing_parameter_id == ijp.id)
                .group_by(ijp.id)
            )
        )

    def add_indexing_parameter(self, conn: Connection, p: DBIndexingParameter) -> int:
        ijp = self.tables.indexing_parameter
        existing_result = conn.execute(
            sa.select([ijp.c.id]).where(ijp.c.project_file_hash == p.project_file_hash)
        ).one()

        if existing_result is not None:
            self.update_indexing_parameter_discovery(
                conn, existing_result[0], datetime.datetime.utcnow()
            )
            return existing_result[0]

        return conn.execute(
            sa.insert(ijp).values(
                project_file_first_discovery=p.project_file_first_discovery,
                project_file_last_discovery=p.project_file_last_discovery,
                project_path=p.project_file_path,
                project_content=p.project_file_content,
                project_hash=p.project_file_hash,
                geometry_file_content=p.geometry_file_content,
            )
        ).inserted_primary_key[0]

    def update_indexing_parameter_discovery(
        self, conn: Connection, parameter_id: int, discovery: datetime.datetime
    ) -> None:
        conn.execute(
            sa.update(self.tables.indexing_parameter)
            .values(project_file_last_discovery=discovery)
            .where(self.tables.indexing_parameter.c.id == parameter_id)
        )

    def add_indexing_job(self, conn: Connection, ij: DBIndexingJob) -> int:
        return conn.execute(
            sa.insert(self.tables.indexing_job).values(
                run_id=ij.run_id,
                started=ij.started,
                output_directory=ij.output_directory,
                stopped=ij.stopped,
                indexing_parameter_id=ij.indexing_parameter_id,
                master_file=ij.master_file,
                command_line=ij.command_line,
                status=ij.status,
                slurm_job_id=ij.slurm_job_id,
                error_message=ij.error_message,
                result_file=ij.result_file,
            )
        ).inserted_primary_key[0]

    def retrieve_indexing_jobs(
        self, conn: Connection, only_running: bool
    ) -> Iterable[DBIndexingJob]:
        ij = self.tables.indexing_job
        ijc = ij.c

        return (
            _convert_job("id", r)
            for r in conn.execute(
                sa.select(
                    [
                        ijc.id,
                        ijc.started,
                        ijc.stopped,
                        ijc.run_id,
                        ijc.indexing_parameter_id,
                        ijc.master_file,
                        ijc.output_directory,
                        ijc.command_line,
                        ijc.status,
                        ijc.slurm_job_id,
                        ijc.error_message,
                        ijc.result_file,
                    ]
                ).where(
                    ijc.status == IndexingJobStatus.RUNNING if only_running else True
                )
            )
        )

    def retrieve_latest_indexing_parameter(
        self, conn: Connection
    ) -> Optional[DBIndexingParameter]:
        ipc = self.tables.indexing_parameter.c
        result = conn.execute(
            sa.select([ipc]).select_from(
                self.tables.configuration.join(
                    self.tables.indexing_parameter,
                    self.tables.configuration.c.latest_indexing_parameter_id == ipc.id,  # type: ignore
                )
            )
        ).one()

        if result is None:
            return None

        return DBIndexingParameter(
            id=result["id"],
            project_file_first_discovery=result["project_file_first_discovery"],
            project_file_last_discovery=result["project_file_last_discovery"],
            project_file_path=result["project_file_path"],
            project_file_content=None,
            geometry_file_content=None,
            project_file_hash=result["project_file_hash"],
        )

    def set_latest_indexing_parameter_id(self, conn: Connection, id_: int) -> None:
        config = conn.execute(
            sa.select([self.tables.configuration.c.latest_indexing_parameter_id])
        ).one()
        conn.execute(
            sa.insert(self.tables.configuration).values(
                latest_indexing_parameter_id=id_
            )
            if config is None
            else sa.update(self.tables.configuration).values(
                latest_indexing_parameter_id=id_
            )
        )

    def retrieve_runs_with_indexing_results(
        self, conn: Connection
    ) -> List[DBRunWithIndexingResult]:
        result: List[DBRunWithIndexingResult] = []
        ijc = self.tables.indexing_job.c
        for _run_id, run_rows in groupby(
            conn.execute(
                sa.select(
                    [
                        self.tables.run.c.id,
                        ijc.id.alias("indexing_job_id"),
                        ijc.started,
                        ijc.stopped,
                        ijc.run_id,
                        ijc.indexing_parameter_id,
                        ijc.master_file,
                        ijc.output_directory,
                        ijc.command_line,
                        ijc.status,
                        ijc.slurm_job_id,
                        ijc.error_message,
                        ijc.result_file,
                    ]
                ).outerjoin(
                    self.tables.indexing_job,
                    self.tables.indexing_job.c.run_id == self.tables.run.c.id,
                )
            ),
            lambda x: x["id"],
        ):
            rows = list(run_rows)
            run_meta = rows[0]
            result.append(
                DBRunWithIndexingResult(
                    run_id=run_meta["id"],
                    indexing_jobs=[_convert_job("indexing_job_id", r) for r in rows],
                )
            )
        return result

    def run_has_indexing_jobs(
        self, conn: Connection, run_id: int, indexing_parameter_id: int
    ) -> bool:
        return (
            conn.execute(
                sa.select([]).where(
                    (self.tables.indexing_job.c.run_id == run_id)
                    & (
                        self.tables.indexing_job.c.indexing_parameter_id
                        == indexing_parameter_id
                    )
                )
            ).one()
            is not None
        )

    def finish_indexing_job_successfully(
        self, conn: Connection, indexing_job_id: int, result_file: Path
    ) -> None:
        ij = self.tables.indexing_job
        conn.execute(
            sa.update(ij)
            .values(
                stopped=datetime.datetime.utcnow(),
                status=IndexingJobStatus.SUCCESS,
                result_file=result_file,
            )
            .where(ij.c.id == indexing_job_id)
        )

    def finish_indexing_job_error(
        self, conn: Connection, indexing_job_id: int, error_message: str
    ) -> None:
        ij = self.tables.indexing_job
        conn.execute(
            sa.update(ij)
            .values(
                stopped=datetime.datetime.utcnow(),
                status=IndexingJobStatus.FAIL,
                error_message=error_message,
            )
            .where(ij.c.id == indexing_job_id)
        )

    def add_jobs_for_parameter_and_runs(
        self,
        conn: Connection,
        indexing_parameter_id: int,
        runs: Iterable[DBIndexingRunData],
    ):
        started = datetime.datetime.utcnow()
        conn.execute(
            sa.insert(self.tables.indexing_job).values(
                [
                    {
                        "started": started,
                        "stopped": None,
                        "run_id": run.run_id,
                        "output_directory": run.output_directory,
                        "indexing_parameter_id": indexing_parameter_id,
                        "master_file": run.master_file,
                        "command_line": run.command_line,
                        "status": IndexingJobStatus.RUNNING,
                        "slurm_job_id": run.slurm_job_id,
                        "error_message": None,
                        "result_file": None,
                    }
                    for run in runs
                ]
            )
        )


def overview_row_to_query_row(
    row: OverviewAttributi, metadata: List[TabledAttributo]
) -> QueryRow:
    return dict_union(
        [
            table_attributi.to_query_row(
                [md.attributo.name for md in metadata if md.table == table],
                prefix=f"{table.value}.",
            )
            for table, table_attributi in row.items()
        ]
    )
