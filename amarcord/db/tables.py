import logging
from typing import Dict

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.modules.dbcontext import DBContext

logger = logging.getLogger(__name__)


def _table_attributo(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Attributo",
        metadata,
        sa.Column("name", sa.String(length=255), primary_key=True),
        sa.Column("description", sa.String(length=255)),
        sa.Column("suffix", sa.String(length=255)),
        sa.Column(
            "associated_table",
            sa.Enum(AssociatedTable),
            nullable=False,
            primary_key=True,
        ),
        sa.Column("json_schema", sa.JSON, nullable=False),
    )


def _table_target(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Target",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id", use_alter=True),
            nullable=True,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("short_name", sa.String(length=255), nullable=False),
        sa.Column("molecular_weight", sa.Float, nullable=True),
        sa.Column("uniprot_id", sa.String(length=64), nullable=True),
    )


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id", use_alter=True),
            nullable=True,
        ),
        sa.Column(
            "target_id",
            sa.Integer,
            sa.ForeignKey("Target.id", use_alter=True),
            nullable=True,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("compounds", sa.JSON, nullable=True),
        sa.Column("micrograph", sa.Text, nullable=True),
        sa.Column("protocol", sa.Text, nullable=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_run_comment(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "RunComment",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id", ondelete="cascade")),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("comment_text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_proposal(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Proposal",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("metadata", sa.JSON, nullable=True),
        sa.Column("admin_password", sa.String(length=255), nullable=True),
    )


def _table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id"),
            nullable=False,
        ),
        sa.Column("sample_id", sa.Integer, ForeignKey("Sample.id"), nullable=True),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_data_source(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "DataSource",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey("Run.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("source", sa.JSON, nullable=True),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("number_of_frames", sa.Integer, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_peak_search_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "PeakSearchParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("method", sa.String(length=255), nullable=False),
        sa.Column("software", sa.String(length=255), nullable=False),
        sa.Column("software_version", sa.String(length=255), nullable=True),
        sa.Column("max_num_peaks", sa.Float, nullable=True),
        sa.Column("adc_threshold", sa.Float, nullable=True),
        sa.Column("minimum_snr", sa.Float, nullable=True),
        sa.Column("min_pixel_count", sa.Integer, nullable=True),
        sa.Column("max_pixel_count", sa.Integer, nullable=True),
        sa.Column("min_res", sa.Float, nullable=True),
        sa.Column("max_res", sa.Float, nullable=True),
        sa.Column("bad_pixel_map_filename", sa.Text, nullable=True),
        sa.Column("bad_pixel_map_hdf5_path", sa.Text, nullable=True),
        sa.Column("local_bg_radius", sa.Float, nullable=True),
        sa.Column("min_peak_over_neighbor", sa.Float, nullable=True),
        sa.Column("min_snr_biggest_pix", sa.Float, nullable=True),
        sa.Column("min_snr_peak_pix", sa.Float, nullable=True),
        sa.Column("min_sig", sa.Float, nullable=True),
        sa.Column("min_squared_gradient", sa.Float, nullable=True),
        sa.Column("geometry", sa.Text, nullable=True),
    )


def _table_hit_finding_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "HitFindingParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("min_peaks", sa.Integer, nullable=False),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("software", sa.String(length=255), nullable=False),
        sa.Column("software_version", sa.String(length=255), nullable=True),
    )


def _table_hit_finding_result(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "HitFindingResult",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "peak_search_parameters_id",
            sa.Integer,
            ForeignKey("PeakSearchParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "hit_finding_parameters_id",
            sa.Integer,
            ForeignKey("HitFindingParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "data_source_id", sa.Integer, ForeignKey("DataSource.id"), nullable=False
        ),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("number_of_hits", sa.Integer, nullable=False),
        sa.Column("hit_rate", sa.Float, nullable=False),
        sa.Column("average_peaks_event", sa.Float, nullable=True),
        sa.Column("average_resolution", sa.Float, nullable=True),
        sa.Column("result_filename", sa.Text, nullable=False),
        sa.Column("result_type", sa.String(length=255), nullable=False),
        sa.Column("peaks_filename", sa.Text, nullable=True),
    )


def _table_indexing_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IndexingParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("software", sa.String(length=255), nullable=False),
        sa.Column("software_version", sa.String(length=255), nullable=True),
        sa.Column("command_line", sa.Text, nullable=False),
        sa.Column("parameters", sa.JSON, nullable=False),
        sa.Column("methods", sa.JSON, nullable=True),
        sa.Column("geometry", sa.Text, nullable=True),
    )


def _table_integration_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IntegrationParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("software", sa.String(length=255), nullable=False),
        sa.Column("software_version", sa.String(length=255), nullable=True),
        sa.Column("method", sa.String(length=255), nullable=True),
        sa.Column("center_boxes", sa.Boolean, nullable=True),
        sa.Column("overpredict", sa.Boolean, nullable=True),
        sa.Column("push_res", sa.Float, nullable=True),
        sa.Column("radius_inner", sa.Integer, nullable=True),
        sa.Column("radius_middle", sa.Integer, nullable=True),
        sa.Column("radius_outer", sa.Integer, nullable=True),
    )


def _table_indexing_result(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IndexingResult",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "hit_finding_result_id",
            sa.Integer,
            ForeignKey("HitFindingResult.id"),
            nullable=False,
        ),
        sa.Column(
            "peak_search_parameters_id",
            sa.Integer,
            ForeignKey("PeakSearchParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "integration_parameters_id",
            sa.Integer,
            ForeignKey("IntegrationParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "indexing_parameters_id",
            sa.Integer,
            ForeignKey("IndexingParameters.id"),
            nullable=False,
        ),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("tag", sa.String(length=255), nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=True),
        sa.Column("result_filename", sa.Text, nullable=True),
        sa.Column("num_indexed", sa.Integer, nullable=False),
        sa.Column("num_crystals", sa.Integer, nullable=False),
    )


def _table_event_log(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "EventLog",
        metadata,
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
        ),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("level", sa.Enum(EventLogLevel), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
    )


class DBTables:
    def __init__(
        self,
        sample: sa.Table,
        proposal: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        attributo: sa.Table,
        target: sa.Table,
        data_source: sa.Table,
        peak_search_parameters: sa.Table,
        hit_finding_parameters: sa.Table,
        hit_finding_results: sa.Table,
        indexing_parameters: sa.Table,
        indexing_results: sa.Table,
        integration_parameters: sa.Table,
        event_log: sa.Table,
    ) -> None:
        self.event_log = event_log
        self.integration_parameters = integration_parameters
        self.indexing_results = indexing_results
        self.indexing_parameters = indexing_parameters
        self.hit_finding_results = hit_finding_results
        self.hit_finding_parameters = hit_finding_parameters
        self.peak_search_parameters = peak_search_parameters
        self.sample = sample
        self.proposal = proposal
        self.run = run
        self.run_comment = run_comment
        self.target = target
        self.data_source = data_source
        self.attributo = attributo
        self.attributo_run_id = AttributoId("id")
        self.attributo_run_comments = AttributoId("comments")
        self.attributo_run_modified = AttributoId("modified")
        self.attributo_run_sample_id = AttributoId("sample_id")
        self.attributo_run_proposal_id = AttributoId("proposal_id")
        self.attributo_run_id = AttributoId("id")
        self.additional_attributi: Dict[
            AssociatedTable, Dict[AttributoId, DBAttributo]
        ] = {
            AssociatedTable.SAMPLE: {
                AttributoId("id"): DBAttributo(
                    name=AttributoId("id"),
                    description="Sample ID",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeInt(),
                ),
                AttributoId("name"): DBAttributo(
                    name=AttributoId("name"),
                    description="Name",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
                AttributoId("created"): DBAttributo(
                    name=AttributoId("created"),
                    description="Created",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeDateTime(),
                ),
                AttributoId("micrograph"): DBAttributo(
                    name=AttributoId("micrograph"),
                    description="Micrograph",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
                AttributoId("protocol"): DBAttributo(
                    name=AttributoId("protocol"),
                    description="Protocol",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
            },
            AssociatedTable.RUN: {
                self.attributo_run_id: DBAttributo(
                    name=self.attributo_run_id,
                    description="Run ID",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeInt(),
                ),
                self.attributo_run_comments: DBAttributo(
                    name=self.attributo_run_comments,
                    description="Comments",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeComments(),
                ),
                self.attributo_run_modified: DBAttributo(
                    name=self.attributo_run_modified,
                    description="Modified",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeDateTime(),
                ),
                self.attributo_run_sample_id: DBAttributo(
                    name=self.attributo_run_sample_id,
                    description="Sample ID",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeSample(),
                ),
                self.attributo_run_proposal_id: DBAttributo(
                    name=self.attributo_run_proposal_id,
                    description="Proposal",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeInt(),
                ),
            },
        }


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    return DBTables(
        sample=_table_sample(metadata),
        proposal=_table_proposal(metadata),
        run=_table_run(metadata),
        run_comment=_table_run_comment(metadata),
        attributo=_table_attributo(metadata),
        target=_table_target(metadata),
        data_source=_table_data_source(metadata),
        peak_search_parameters=_table_peak_search_parameters(metadata),
        hit_finding_parameters=_table_hit_finding_parameters(metadata),
        hit_finding_results=_table_hit_finding_result(metadata),
        indexing_parameters=_table_indexing_parameters(metadata),
        indexing_results=_table_indexing_result(metadata),
        integration_parameters=_table_integration_parameters(metadata),
        event_log=_table_event_log(metadata),
    )


def create_tables(context: DBContext) -> DBTables:
    return create_tables_from_metadata(context.metadata)
