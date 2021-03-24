import logging
from typing import Dict

import sqlalchemy as sa
from sqlalchemy import ForeignKey

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.dbattributo import DBAttributo
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
            "target_id",
            sa.Integer,
            sa.ForeignKey("Target.id", use_alter=True),
            nullable=True,
        ),
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
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("number_of_frames", sa.Integer, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_peak_search_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "PeakSearchParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "data_source_id", sa.Integer, ForeignKey("DataSource.id"), nullable=False
        ),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("method", sa.String, nullable=False),
        sa.Column("software", sa.String, nullable=False),
        sa.Column("software_version", sa.String, nullable=True),
        sa.Column("software_git_repository", sa.Text, nullable=True),
        sa.Column("software_git_sha", sa.String, nullable=True),
        sa.Column("command_line", sa.Text, nullable=False),
        sa.Column("max_num_peaks", sa.Float, nullable=True),
        sa.Column("adc_threshold", sa.Float, nullable=True),
        sa.Column("minimum_snr", sa.Float, nullable=True),
        sa.Column("min_pixel_count", sa.Float, nullable=True),
        sa.Column("max_pixel_count", sa.Float, nullable=True),
        sa.Column("min_res", sa.Float, nullable=True),
        sa.Column("max_res", sa.Float, nullable=True),
        sa.Column("bad_pixel_filename", sa.Text, nullable=True),
        sa.Column("local_bg_radius", sa.Float, nullable=True),
        sa.Column("min_peak_over_neighbor", sa.Float, nullable=True),
        sa.Column("min_snr_biggest_pix", sa.Float, nullable=True),
        sa.Column("min_snr_peak_pix", sa.Float, nullable=True),
        sa.Column("min_sig", sa.Float, nullable=True),
        sa.Column("min_squared_gradient", sa.Float, nullable=True),
        sa.Column("geometry_filename", sa.Text, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_hit_finding_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "HitFindingParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("min_peaks", sa.Integer, nullable=False),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_hit_finding_results(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "HitFindingResults",
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
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("number_of_hits", sa.Integer, nullable=False),
        sa.Column("hit_rate", sa.Float, nullable=False),
        sa.Column("result_filename", sa.Text, nullable=False),
    )


def _table_indexing_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IndexingParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "hit_finding_results_id",
            sa.Integer,
            ForeignKey("HitFindingResults.id"),
            nullable=False,
        ),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("software", sa.String, nullable=False),
        sa.Column("software_version", sa.String, nullable=True),
        sa.Column("software_git_repository", sa.Text, nullable=True),
        sa.Column("software_git_sha", sa.String, nullable=True),
        sa.Column("command_line", sa.Text, nullable=False),
        sa.Column("parameters", sa.JSON, nullable=False),
        sa.Column("methods", sa.JSON, nullable=True),
        sa.Column("target_cell_filename", sa.Text, nullable=True),
        sa.Column("geometry", sa.Text, nullable=True),
        sa.Column("geometry_filename", sa.Text, nullable=True),
    )


def _table_integration_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IntegrationParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("method", sa.String, nullable=True),
        sa.Column("center_boxes", sa.Boolean, nullable=True),
        sa.Column("overpredict", sa.Boolean, nullable=True),
        sa.Column("push_res", sa.Float, nullable=True),
        sa.Column("radius_inner", sa.Integer, nullable=True),
        sa.Column("radius_middle", sa.Integer, nullable=True),
        sa.Column("radius_outer", sa.Integer, nullable=True),
    )


def _table_ambiguity_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "AmbiguityParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("software", sa.String, nullable=False),
        sa.Column("software_version", sa.String, nullable=True),
        sa.Column("software_git_repository", sa.Text, nullable=True),
        sa.Column("software_git_sha", sa.String, nullable=True),
        sa.Column("parameters", sa.JSON, nullable=False),
    )


def _table_indexing_results(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "IndexingResults",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "indexing_parameters_id",
            sa.Integer,
            ForeignKey("IndexingParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "integration_parameters_id",
            sa.Integer,
            ForeignKey("IntegrationParameters.id"),
            nullable=False,
        ),
        sa.Column(
            "ambiguity_parameters_id",
            sa.Integer,
            ForeignKey("AmbiguityParameters.id"),
            nullable=True,
        ),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("result_filename", sa.Text, nullable=True),
        sa.Column("num_indexed", sa.Integer, nullable=False),
        sa.Column("num_crystals", sa.Integer, nullable=False),
    )


def _table_merge_parameters(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "MergeParameters",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tag", sa.String, nullable=True),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("software", sa.String, nullable=False),
        sa.Column("software_version", sa.String, nullable=True),
        sa.Column("software_git_repository", sa.Text, nullable=True),
        sa.Column("software_git_sha", sa.String, nullable=True),
        sa.Column("command_line", sa.Text, nullable=False),
        sa.Column("parameters", sa.JSON, nullable=False),
        sa.Column("partiality_model", sa.String, nullable=True),
        sa.Column("num_iterations", sa.Integer, nullable=True),
        sa.Column("scale_linear", sa.Boolean, nullable=True),
        sa.Column("scale_bfactor", sa.Boolean, nullable=True),
        sa.Column("post_refine", sa.Boolean, nullable=True),
        sa.Column("symmetry", sa.String, nullable=True),
        sa.Column("polarization", sa.String, nullable=True),
        sa.Column("min_measurements", sa.Integer, nullable=True),
        sa.Column("max_adu", sa.Float, nullable=True),
        sa.Column("min_res", sa.Float, nullable=True),
    )


def _table_merge_has_indexing(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "MergeHasIndexing",
        metadata,
        sa.Column(
            "merge_results_id",
            sa.Integer,
            ForeignKey("MergeResults.id"),
            primary_key=True,
        ),
        sa.Column(
            "indexing_results_id",
            sa.Integer,
            ForeignKey("IndexingResults.id"),
            primary_key=True,
        ),
    )


def _table_merge_results(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "MergeResults",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_parameters_id",
            sa.Integer,
            ForeignKey("MergeParameters.id"),
            nullable=False,
        ),
        sa.Column("result_hkl_filename", sa.Text, nullable=True),
        sa.Column("result_mtz_filename", sa.Text, nullable=True),
        sa.Column("rsplit", sa.Float, nullable=False),
        sa.Column("cc_half", sa.Float, nullable=False),
        sa.Column("comment", sa.String, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
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
        merge_parameters: sa.Table,
        merge_results: sa.Table,
        ambiguity_parameters: sa.Table,
        merge_has_indexing: sa.Table,
    ) -> None:
        self.merge_has_indexing = merge_has_indexing
        self.ambiguity_parameters = ambiguity_parameters
        self.merge_results = merge_results
        self.merge_parameters = merge_parameters
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
                AttributoId("created"): DBAttributo(
                    name=AttributoId("created"),
                    description="Created",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeDateTime(),
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
        self.attributo_attributi = AttributoId(self.run.c.attributi.name)


def create_tables(context: DBContext) -> DBTables:
    return DBTables(
        sample=_table_sample(context.metadata),
        proposal=_table_proposal(context.metadata),
        run=_table_run(context.metadata),
        run_comment=_table_run_comment(context.metadata),
        attributo=_table_attributo(context.metadata),
        target=_table_target(context.metadata),
        data_source=_table_data_source(context.metadata),
        peak_search_parameters=_table_peak_search_parameters(context.metadata),
        hit_finding_parameters=_table_hit_finding_parameters(context.metadata),
        hit_finding_results=_table_hit_finding_results(context.metadata),
        indexing_parameters=_table_indexing_parameters(context.metadata),
        indexing_results=_table_indexing_results(context.metadata),
        integration_parameters=_table_integration_parameters(context.metadata),
        merge_parameters=_table_merge_parameters(context.metadata),
        merge_results=_table_merge_results(context.metadata),
        ambiguity_parameters=_table_ambiguity_parameters(context.metadata),
        merge_has_indexing=_table_merge_has_indexing(context.metadata),
    )
