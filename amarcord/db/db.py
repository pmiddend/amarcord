import datetime
import logging
import pickle
import re
from dataclasses import dataclass
from itertools import groupby
from typing import Any
from typing import Dict
from typing import Final
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
from amarcord.db.attributo_id import (
    AttributoId,
)
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.comment import DBComment
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.constants import DB_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.karabo import Karabo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
from amarcord.db.table_classes import DBMergeParameters
from amarcord.db.table_classes import DBMergeResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBSample
from amarcord.db.table_classes import DBSampleAnalysisResult
from amarcord.db.table_classes import DBTarget
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import (
    DBTables,
)
from amarcord.modules.dbcontext import DBContext
from amarcord.query_parser import Row as QueryRow
from amarcord.util import dict_union
from amarcord.util import remove_duplicates_stable

logger = logging.getLogger(__name__)

VALIDATE_ATTRIBUTI: Final = True

Connection = Any


OverviewAttributi = Dict[AssociatedTable, AttributiMap]


@dataclass(frozen=True)
class TableWithAttributi:
    table: AssociatedTable
    attributi: Dict[AttributoId, DBAttributo]


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
            for k in self.retrieve_samples(conn)
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
            where_condition = and_(where_condition, run.c.modified >= since)
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

    def retrieve_mini_samples(self, conn: Connection) -> List[DBMiniSample]:
        return [
            DBMiniSample(row[0], row[1])
            for row in conn.execute(
                sa.select(
                    [self.tables.sample.c.id, self.tables.sample.c.name]
                ).order_by(self.tables.sample.c.name)
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

    def retrieve_run(self, conn: Connection, run_id: int) -> DBRun:
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
            .where(run_c.id == run_id)
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

    def add_comment(self, conn: Connection, run_id: int, author: str, text: str) -> int:
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
                created=datetime.datetime.utcnow(),
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

    def update_run_karabo(self, conn: Connection, run_id: int, karabo: bytes) -> None:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(karabo=karabo, modified=datetime.datetime.utcnow())
        )

    def retrieve_attributi(
        self, conn: Connection, filter_table: Optional[AssociatedTable] = None
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
        for table, attributi in self.tables.additional_attributi.items():
            if table not in result:
                result[table] = attributi
            else:
                result[table].update(attributi)
        return result

    def retrieve_table_attributi(
        self, conn: Connection, table: AssociatedTable
    ) -> Dict[AttributoId, DBAttributo]:
        return self.retrieve_attributi(conn, table)[table]

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
    ) -> None:
        if attributo == AttributoId("sample_id"):
            assert isinstance(value, int)
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
            attributi_map.set_single_manual(attributo, value)
        else:
            attributi_map.remove_manual_attributo(attributo)
        self.update_run_attributi(conn, run_id, attributi_map)

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    # pylint: disable=no-self-use
    # noinspection PyMethodMayBeStatic
    def retrieve_karabo(self, _conn: Connection, _run_id: int) -> Optional[Karabo]:
        result: Optional[bytes]
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
        if not re.fullmatch(ATTRIBUTO_NAME_REGEX, name):
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

    def retrieve_targets(self, conn: Connection) -> List[DBTarget]:
        tc = self.tables.target.c
        return [
            DBTarget(
                row["id"],
                row["name"],
                row["short_name"],
                row["molecular_weight"],
                row["uniprot_id"],
            )
            for row in conn.execute(
                sa.select(
                    [tc.id, tc.name, tc.short_name, tc.molecular_weight, tc.uniprot_id]
                ).order_by(tc.short_name)
            ).fetchall()
        ]

    def add_target(self, conn: Connection, t: DBTarget) -> int:
        assert t.name.strip()
        assert t.short_name.strip()
        assert t.id is None
        assert t.molecular_weight is None or t.molecular_weight >= 0
        return conn.execute(
            sa.insert(self.tables.target).values(
                name=t.name,
                short_name=t.short_name,
                molecular_weight=t.molecular_weight,
                uniprot_id=t.uniprot_id,
            )
        ).inserted_primary_key[0]

    def edit_target(self, conn: Connection, t: DBTarget) -> None:
        assert t.name.strip()
        assert t.short_name.strip()
        assert t.id is not None
        assert t.molecular_weight is None or t.molecular_weight >= 0
        conn.execute(
            sa.update(self.tables.target)
            .values(
                name=t.name,
                short_name=t.short_name,
                molecular_weight=t.molecular_weight,
                uniprot_id=t.uniprot_id,
            )
            .where(self.tables.target.c.id == t.id)
        )

    def delete_target(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.target).where(self.tables.target.c.id == tid)
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
        self, conn: Connection, since: Optional[datetime.datetime] = None
    ) -> List[DBSample]:
        tc = self.tables.sample.c
        select_stmt = sa.select(
            [
                tc.id,
                tc.name,
                tc.target_id,
                tc.compounds,
                tc.micrograph,
                tc.protocol,
                tc.attributi,
            ]
        ).order_by(tc.id)
        if since is not None:
            select_stmt = select_stmt.where(tc.modified >= since)

        def prepare_sample(row: Any) -> DBSample:
            return DBSample(
                id=row["id"],
                name=row["name"],
                target_id=row["target_id"],
                compounds=row["compounds"],
                micrograph=row["micrograph"],
                protocol=row["protocol"],
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
                target_id=t.target_id,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
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
                target_id=t.target_id,
                name=t.name,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
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
                for sample in self.retrieve_samples(conn, since=None):
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

    def add_data_source(self, conn: Connection, ds: DBDataSource) -> int:
        return conn.execute(
            sa.insert(self.tables.data_source).values(
                run_id=ds.run_id,
                number_of_frames=ds.number_of_frames,
                source=ds.source,
                tag=ds.tag,
                comment=ds.comment,
            )
        ).inserted_primary_key[0]

    def add_hit_finding_result(self, conn: Connection, ds: DBHitFindingResult) -> int:
        with conn.begin():
            peak_search_id = conn.execute(
                sa.insert(self.tables.peak_search_parameters).values(
                    data_source_id=ds.data_source_id,
                    method=ds.peak_search_parameters.method,
                    software=ds.peak_search_parameters.software,
                    command_line=ds.peak_search_parameters.command_line,
                    tag=ds.peak_search_parameters.tag,
                    comment=ds.peak_search_parameters.comment,
                    software_version=ds.peak_search_parameters.software_version,
                    software_git_repository=ds.peak_search_parameters.software_git_repository,
                    software_git_sha=ds.peak_search_parameters.software_git_sha,
                    max_num_peaks=ds.peak_search_parameters.max_num_peaks,
                    adc_threshold=ds.peak_search_parameters.adc_threshold,
                    minimum_snr=ds.peak_search_parameters.minimum_snr,
                    min_pixel_count=ds.peak_search_parameters.min_pixel_count,
                    max_pixel_count=ds.peak_search_parameters.max_pixel_count,
                    min_res=ds.peak_search_parameters.min_res,
                    max_res=ds.peak_search_parameters.max_res,
                    bad_pixel_filename=ds.peak_search_parameters.bad_pixel_filename,
                    local_bg_radius=ds.peak_search_parameters.local_bg_radius,
                    min_peak_over_neighbor=ds.peak_search_parameters.min_peak_over_neighbor,
                    min_snr_biggest_pix=ds.peak_search_parameters.min_snr_biggest_pix,
                    min_snr_peak_pix=ds.peak_search_parameters.min_snr_peak_pix,
                    min_sig=ds.peak_search_parameters.min_sig,
                    min_squared_gradient=ds.peak_search_parameters.min_squared_gradient,
                    geometry_filename=ds.peak_search_parameters.geometry_filename,
                )
            ).inserted_primary_key[0]

            hit_finding_param_id = conn.execute(
                sa.insert(self.tables.hit_finding_parameters).values(
                    min_peaks=ds.hit_finding_parameters.min_peaks,
                    tag=ds.hit_finding_parameters.tag,
                    comment=ds.hit_finding_parameters.comment,
                )
            ).inserted_primary_key[0]

            return conn.execute(
                sa.insert(self.tables.hit_finding_results).values(
                    peak_search_parameters_id=peak_search_id,
                    hit_finding_parameters_id=hit_finding_param_id,
                    result_filename=ds.result_filename,
                    number_of_hits=ds.number_of_hits,
                    hit_rate=ds.hit_rate,
                    tag=ds.tag,
                    comment=ds.comment,
                )
            ).inserted_primary_key[0]

    def add_indexing_result(self, conn: Connection, ds: DBIndexingResult) -> int:
        with conn.begin():
            indexing_parameters_id = conn.execute(
                sa.insert(self.tables.indexing_parameters).values(
                    hit_finding_results_id=ds.hit_finding_results_id,
                    software=ds.indexing_parameters.software,
                    command_line=ds.indexing_parameters.command_line,
                    parameters=ds.indexing_parameters.parameters,
                    tag=ds.tag,
                    comment=ds.comment,
                )
            ).inserted_primary_key[0]

            integration_parameters_id = conn.execute(
                sa.insert(self.tables.integration_parameters).values()
            ).inserted_primary_key[0]

            return conn.execute(
                sa.insert(self.tables.indexing_results).values(
                    indexing_parameters_id=indexing_parameters_id,
                    integration_parameters_id=integration_parameters_id,
                    num_indexed=ds.num_indexed,
                    num_crystals=ds.num_crystals,
                )
            ).inserted_primary_key[0]

    def add_merge_result(self, conn: Connection, mr: DBMergeResult) -> None:
        with conn.begin():
            merge_parameters_id = conn.execute(
                sa.insert(self.tables.merge_parameters).values(
                    software=mr.merge_parameters.software,
                    command_line=mr.merge_parameters.command_line,
                    parameters=mr.merge_parameters.parameters,
                )
            ).inserted_primary_key[0]

            merge_results_id = conn.execute(
                sa.insert(self.tables.merge_results).values(
                    merge_parameters_id=merge_parameters_id,
                    rsplit=mr.rsplit,
                    cc_half=mr.cc_half,
                )
            ).inserted_primary_key[0]

            for i in mr.indexing_result_ids:
                conn.execute(
                    sa.insert(self.tables.merge_has_indexing).values(
                        merge_results_id=merge_results_id, indexing_results_id=i
                    )
                )

    def retrieve_analysis_data_sources(self, conn: Connection) -> List[DBDataSource]:
        data_sources: List[DBDataSource] = []
        data_source_id_to_hit_finding_results: Dict[int, List[DBHitFindingResult]] = {}
        hit_finding_results_to_indexing_results: Dict[int, List[DBIndexingResult]] = {}
        for r in conn.execute(
            sa.select(
                [
                    self.tables.data_source.c.id,
                    self.tables.data_source.c.run_id,
                    self.tables.data_source.c.number_of_frames,
                    self.tables.data_source.c.source,
                    self.tables.data_source.c.tag,
                    self.tables.data_source.c.comment,
                ]
            )
        ).fetchall():
            hit_finding_result_list: List[DBHitFindingResult] = []
            data_sources.append(
                DBDataSource(
                    id=r["id"],
                    run_id=r["run_id"],
                    number_of_frames=r["number_of_frames"],
                    hit_finding_results=hit_finding_result_list,
                    source=r["source"],
                    tag=r["tag"],
                    comment=r["comment"],
                )
            )
            data_source_id_to_hit_finding_results[r["id"]] = hit_finding_result_list

        for r in conn.execute(
            sa.select(
                [
                    self.tables.data_source.c.id.label("data_source_id"),
                    self.tables.peak_search_parameters.c.id.label("psp_id"),
                    self.tables.peak_search_parameters.c.method,
                    self.tables.peak_search_parameters.c.software,
                    self.tables.peak_search_parameters.c.command_line,
                    self.tables.peak_search_parameters.c.tag.label("psp_tag"),
                    self.tables.peak_search_parameters.c.comment.label("psp_comment"),
                    self.tables.peak_search_parameters.c.software_version,
                    self.tables.peak_search_parameters.c.software_git_repository,
                    self.tables.peak_search_parameters.c.software_git_sha,
                    self.tables.peak_search_parameters.c.max_num_peaks,
                    self.tables.peak_search_parameters.c.adc_threshold,
                    self.tables.peak_search_parameters.c.minimum_snr,
                    self.tables.peak_search_parameters.c.min_pixel_count,
                    self.tables.peak_search_parameters.c.max_pixel_count,
                    self.tables.peak_search_parameters.c.min_res,
                    self.tables.peak_search_parameters.c.max_res,
                    self.tables.peak_search_parameters.c.bad_pixel_filename,
                    self.tables.peak_search_parameters.c.local_bg_radius,
                    self.tables.peak_search_parameters.c.min_peak_over_neighbor,
                    self.tables.peak_search_parameters.c.min_snr_biggest_pix,
                    self.tables.peak_search_parameters.c.min_snr_peak_pix,
                    self.tables.peak_search_parameters.c.min_sig,
                    self.tables.peak_search_parameters.c.min_squared_gradient,
                    self.tables.peak_search_parameters.c.geometry_filename,
                    self.tables.hit_finding_parameters.c.id.label("hfp_id"),
                    self.tables.hit_finding_parameters.c.tag.label("hfp_tag"),
                    self.tables.hit_finding_parameters.c.comment.label("hfp_comment"),
                    self.tables.hit_finding_parameters.c.min_peaks.label("min_peaks"),
                    self.tables.hit_finding_results.c.id,
                    self.tables.hit_finding_results.c.number_of_hits,
                    self.tables.hit_finding_results.c.hit_rate,
                    self.tables.hit_finding_results.c.result_filename,
                    self.tables.hit_finding_results.c.tag,
                    self.tables.hit_finding_results.c.comment,
                ]
            ).select_from(
                self.tables.data_source.join(self.tables.peak_search_parameters)
                .join(self.tables.hit_finding_results)
                .join(self.tables.hit_finding_parameters)
            )
        ).fetchall():
            # TODO: This loop only works if PeakSearchParameters and IndexingParameters isn't shared among results
            # which I suppose is fine.
            indexing_result_list: List[DBIndexingResult] = []
            data_source_id_to_hit_finding_results[r["data_source_id"]].append(
                DBHitFindingResult(
                    id=r["id"],
                    data_source_id=r["data_source_id"],
                    peak_search_parameters=DBPeakSearchParameters(
                        id=r["psp_id"],
                        method=r["method"],
                        software=r["software"],
                        command_line=r["command_line"],
                        tag=r["psp_tag"],
                        comment=r["psp_comment"],
                        software_version=r["software_version"],
                        software_git_repository=r["software_git_repository"],
                        software_git_sha=r["software_git_sha"],
                        max_num_peaks=r["max_num_peaks"],
                        adc_threshold=r["adc_threshold"],
                        minimum_snr=r["minimum_snr"],
                        min_pixel_count=r["min_pixel_count"],
                        max_pixel_count=r["max_pixel_count"],
                        min_res=r["min_res"],
                        max_res=r["max_res"],
                        bad_pixel_filename=r["bad_pixel_filename"],
                        local_bg_radius=r["local_bg_radius"],
                        min_peak_over_neighbor=r["min_peak_over_neighbor"],
                        min_snr_biggest_pix=r["min_snr_biggest_pix"],
                        min_snr_peak_pix=r["min_snr_peak_pix"],
                        min_sig=r["min_sig"],
                        min_squared_gradient=r["min_squared_gradient"],
                        geometry_filename=r["geometry_filename"],
                    ),
                    hit_finding_parameters=DBHitFindingParameters(
                        id=r["hfp_id"],
                        min_peaks=r["min_peaks"],
                        tag=r["hfp_tag"],
                        comment=r["hfp_comment"],
                    ),
                    result_filename=r["result_filename"],
                    number_of_hits=r["number_of_hits"],
                    hit_rate=r["hit_rate"],
                    indexing_results=indexing_result_list,
                    tag=r["tag"],
                    comment=r["comment"],
                )
            )
            hit_finding_results_to_indexing_results[r["id"]] = indexing_result_list

        for r in conn.execute(
            sa.select(
                [
                    self.tables.indexing_parameters.c.hit_finding_results_id,
                    self.tables.indexing_parameters.c.software,
                    self.tables.indexing_parameters.c.command_line,
                    self.tables.indexing_parameters.c.parameters,
                    self.tables.indexing_results.c.id,
                    self.tables.indexing_results.c.num_indexed,
                    self.tables.indexing_results.c.num_crystals,
                    self.tables.indexing_results.c.tag,
                    self.tables.indexing_results.c.comment,
                ]
            ).select_from(
                self.tables.hit_finding_results.join(
                    self.tables.indexing_parameters
                ).join(self.tables.indexing_results)
            )
        ).fetchall():
            hit_finding_results_to_indexing_results[r["hit_finding_results_id"]].append(
                DBIndexingResult(
                    id=r["id"],
                    hit_finding_results_id=r["hit_finding_results_id"],
                    indexing_parameters=DBIndexingParameters(
                        software=r["software"],
                        command_line=r["command_line"],
                        parameters=r["parameters"],
                    ),
                    integration_parameters=DBIntegrationParameters(),
                    ambiguity_parameters=None,
                    num_indexed=r["num_indexed"],
                    num_crystals=r["num_crystals"],
                    tag=r["tag"],
                    comment=r["comment"],
                )
            )

        return data_sources

    def retrieve_sample_based_analysis(
        self, conn: Connection
    ) -> List[DBSampleAnalysisResult]:
        data_sources: Dict[int, DBDataSource] = {
            cast(int, k.id): k for k in self.retrieve_analysis_data_sources(conn)
        }

        sample_id_to_name: Dict[int, str] = {}
        sample_id_to_data_sources: Dict[int, List[DBDataSource]] = {}
        for row in conn.execute(
            sa.select(
                [
                    self.tables.sample.c.id.label("sample_id"),
                    self.tables.sample.c.name.label("sample_name"),
                    self.tables.data_source.c.id,
                ]
            ).select_from(
                self.tables.sample.outerjoin(self.tables.run).outerjoin(
                    self.tables.data_source
                )
            )
        ).fetchall():
            sample_id = row["sample_id"]
            sample_name = row["sample_name"]
            sample_id_to_name[sample_id] = sample_name
            if sample_id not in sample_id_to_data_sources:
                sample_id_to_data_sources[sample_id] = []
            # Filters samples without runs
            if row["id"] is not None:
                sample_id_to_data_sources[sample_id].append(data_sources[row["id"]])

        sample_to_merge_results = self.retrieve_analysis_merge_results(conn)

        result: List[DBSampleAnalysisResult] = []
        for sample_id, indexing_paths in sample_id_to_data_sources.items():
            result.append(
                DBSampleAnalysisResult(
                    sample_id,
                    sample_id_to_name[sample_id],
                    indexing_paths=indexing_paths,
                    merge_results=sample_to_merge_results.get(sample_id, []),
                )
            )
        return result

    def retrieve_analysis_merge_results(
        self, conn: Connection
    ) -> Dict[int, List[DBMergeResult]]:
        sample_to_merge_results: Dict[int, List[DBMergeResult]] = {}
        for merge_result_id, results in groupby(
            conn.execute(
                sa.select(
                    [
                        self.tables.sample.c.id.label("sample_id"),
                        self.tables.merge_parameters.c.software,
                        self.tables.merge_parameters.c.command_line,
                        self.tables.merge_parameters.c.parameters,
                        self.tables.merge_results.c.id,
                        self.tables.merge_results.c.cc_half,
                        self.tables.merge_results.c.rsplit,
                        self.tables.merge_has_indexing.c.indexing_results_id,
                    ]
                )
                .select_from(
                    self.tables.merge_results.join(self.tables.merge_has_indexing)
                    .join(self.tables.merge_parameters)
                    .join(self.tables.indexing_results)
                    .join(self.tables.indexing_parameters)
                    .join(self.tables.hit_finding_results)
                    .join(self.tables.peak_search_parameters)
                    .join(self.tables.data_source)
                    .join(self.tables.run)
                    .join(self.tables.sample)
                )
                .order_by(self.tables.merge_results.c.id)
            ).fetchall(),
            lambda x: x["id"],
        ):
            results_list = list(results)
            first_row = results_list[0]
            if first_row["sample_id"] not in sample_to_merge_results:
                sample_to_merge_results[first_row["sample_id"]] = []
            sample_to_merge_results[first_row["sample_id"]].append(
                DBMergeResult(
                    id=merge_result_id,
                    merge_parameters=DBMergeParameters(
                        software=first_row["software"],
                        command_line=first_row["command_line"],
                        parameters=first_row["parameters"],
                    ),
                    indexing_result_ids=[
                        r["indexing_results_id"] for r in results_list
                    ],
                    rsplit=first_row["rsplit"],
                    cc_half=first_row["cc_half"],
                )
            )
        return sample_to_merge_results

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
