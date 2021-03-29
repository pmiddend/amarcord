from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
from amarcord.db.table_classes import DBMergeParameters
from amarcord.db.table_classes import DBMergeResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.table_classes import DBSample

RUN_ID = 1

PROPOSAL_ID = ProposalId(1)


def test_retrieve_analysis_only_singletons(db: DB) -> None:
    """
    Create "one of everything" (indexing, hit finding) and check if we have that reflected in the DB
    """
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        sample_id = db.add_sample(
            conn, DBSample(id=None, name="sample1", attributi=RawAttributiMap({}))
        )
        db.add_run(
            conn,
            PROPOSAL_ID,
            run_id=RUN_ID,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        data_source_id = db.add_data_source(
            conn,
            DBDataSource(
                id=None, run_id=RUN_ID, number_of_frames=10, hit_finding_results=[]
            ),
        )
        hit_finding_result_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                data_source_id=data_source_id,
                peak_search_parameters=DBPeakSearchParameters(
                    id=None, method="dummy", software="dummy", command_line="dummy"
                ),
                hit_finding_parameters=DBHitFindingParameters(
                    id=None, min_peaks=1, tag=None, comment=None
                ),
                result_filename="/tmp/test.hkl",
                number_of_hits=2,
                hit_rate=0.2,
                indexing_results=[],
            ),
        )
        indexing_result_id = db.add_indexing_result(
            conn,
            DBIndexingResult(
                id=None,
                hit_finding_results_id=hit_finding_result_id,
                indexing_parameters=DBIndexingParameters(
                    software="dummy", command_line="dummy", parameters={}
                ),
                integration_parameters=DBIntegrationParameters(),
                ambiguity_parameters=None,
                num_indexed=1,
                num_crystals=1,
                tag="",
                comment="",
            ),
        )
        db.add_merge_result(
            conn,
            DBMergeResult(
                id=None,
                merge_parameters=DBMergeParameters(
                    software="dummy", command_line="dummy", parameters={}
                ),
                indexing_result_ids=[indexing_result_id],
                rsplit=0.5,
                cc_half=0.8,
            ),
        )

        analysis = db.retrieve_sample_based_analysis(conn)

        assert len(analysis) == 1
        assert analysis[0].sample_id == sample_id
        assert len(analysis[0].merge_results) == 1
        assert len(analysis[0].indexing_paths) == 1
        assert analysis[0].indexing_paths[0].run_id == RUN_ID
        assert len(analysis[0].indexing_paths[0].hit_finding_results) == 1
        assert (
            len(analysis[0].indexing_paths[0].hit_finding_results[0].indexing_results)
            == 1
        )


def test_retrieve_analysis(db: DB) -> None:
    """
    Create "one of everything" as above, and then another data source that only has hit finding results so far
    """
    with db.connect() as conn:
        db.add_proposal(conn, PROPOSAL_ID)

        sample_id = db.add_sample(
            conn, DBSample(id=None, name="sample1", attributi=RawAttributiMap({}))
        )
        db.add_run(
            conn,
            PROPOSAL_ID,
            run_id=RUN_ID,
            sample_id=sample_id,
            attributi=RawAttributiMap({}),
        )
        data_source_id = db.add_data_source(
            conn,
            DBDataSource(
                id=None, run_id=RUN_ID, number_of_frames=10, hit_finding_results=[]
            ),
        )
        hit_finding_result_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                None,
                data_source_id,
                DBPeakSearchParameters(
                    id=None, method="dummy", software="dummy", command_line="dummy"
                ),
                hit_finding_parameters=DBHitFindingParameters(
                    id=None, min_peaks=1, tag=None, comment=None
                ),
                result_filename="/tmp/test.hkl",
                number_of_hits=2,
                hit_rate=0.2,
                indexing_results=[],
            ),
        )
        indexing_result_id = db.add_indexing_result(
            conn,
            DBIndexingResult(
                id=None,
                hit_finding_results_id=hit_finding_result_id,
                indexing_parameters=DBIndexingParameters(
                    software="dummy", command_line="dummy", parameters={}
                ),
                integration_parameters=DBIntegrationParameters(),
                ambiguity_parameters=None,
                num_indexed=1,
                num_crystals=1,
                tag="",
                comment="",
            ),
        )
        db.add_merge_result(
            conn,
            DBMergeResult(
                id=None,
                merge_parameters=DBMergeParameters(
                    software="dummy", command_line="dummy", parameters={}
                ),
                indexing_result_ids=[indexing_result_id],
                rsplit=0.5,
                cc_half=0.8,
            ),
        )

        data_source_2_id = db.add_data_source(
            conn,
            DBDataSource(
                id=None, run_id=RUN_ID, number_of_frames=10, hit_finding_results=[]
            ),
        )
        _hit_finding_result_2_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                data_source_id=data_source_2_id,
                peak_search_parameters=DBPeakSearchParameters(
                    id=None, method="dummy", software="dummy", command_line="dummy"
                ),
                hit_finding_parameters=DBHitFindingParameters(
                    id=None, min_peaks=1, tag=None, comment=None
                ),
                result_filename="/tmp/foo.hkl",
                number_of_hits=10,
                hit_rate=0.2,
                indexing_results=[],
            ),
        )

        analysis = db.retrieve_sample_based_analysis(conn)

        assert len(analysis) == 1
        assert analysis[0].sample_id == sample_id
        assert len(analysis[0].merge_results) == 1
        assert len(analysis[0].indexing_paths) == 2
