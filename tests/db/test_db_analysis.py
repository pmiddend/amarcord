from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
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
            DBDataSource(id=None, run_id=RUN_ID, number_of_frames=10),
        )
        psp_id = db.add_peak_search_parameters(
            conn,
            DBPeakSearchParameters(id=None, method="dummy", software="dummy"),
        )
        hfp_id = db.add_hit_finding_parameters(
            conn,
            DBHitFindingParameters(
                id=None, min_peaks=1, tag=None, comment=None, software=""
            ),
        )
        hit_finding_result_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                data_source_id=data_source_id,
                peak_search_parameters_id=psp_id,
                hit_finding_parameters_id=hfp_id,
                result_filename="/tmp/test.hkl",
                result_type="",
                average_resolution=0,
                average_peaks_event=0,
                number_of_hits=2,
                hit_rate=0.2,
            ),
        )
        ip_id = db.add_indexing_parameters(
            conn,
            DBIndexingParameters(
                id=None,
                tag=None,
                comment=None,
                software="dummy",
                software_version=None,
                command_line="dummy",
                parameters={},
                methods=[],
                geometry=None,
            ),
        )
        intp_id = db.add_integration_parameters(
            conn,
            DBIntegrationParameters(
                id=None,
                tag=None,
                comment=None,
                software="",
                software_version=None,
                method=None,
                center_boxes=None,
                overpredict=None,
                push_res=None,
                radius_outer=None,
                radius_inner=None,
                radius_middle=None,
            ),
        )
        indexing_result_id = db.add_indexing_result(
            conn,
            DBIndexingResult(
                id=None,
                peak_search_parameters_id=psp_id,
                hit_finding_result_id=hit_finding_result_id,
                indexing_parameters_id=ip_id,
                integration_parameters_id=intp_id,
                num_indexed=1,
                num_crystals=1,
                tag="",
                comment="",
                result_filename="",
            ),
        )
        analysis = db.retrieve_sample_based_analysis(conn)

        assert len(analysis) == 1
        assert analysis[0].sample_id == sample_id
        assert len(analysis[0].data_sources) == 1
        assert analysis[0].data_sources[0].data_source.run_id == RUN_ID
        assert len(analysis[0].data_sources[0].hit_finding_results) == 1
        assert (
            len(analysis[0].data_sources[0].hit_finding_results[0].indexing_results)
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
            DBDataSource(id=None, run_id=RUN_ID, number_of_frames=10),
        )
        psp_id = db.add_peak_search_parameters(
            conn, DBPeakSearchParameters(id=None, method="dummy", software="dummy")
        )
        hfp_id = db.add_hit_finding_parameters(
            conn,
            DBHitFindingParameters(
                id=None,
                min_peaks=1,
                tag=None,
                comment=None,
                software="",
                software_version="",
            ),
        )
        hit_finding_result_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                data_source_id=data_source_id,
                peak_search_parameters_id=psp_id,
                hit_finding_parameters_id=hfp_id,
                result_filename="/tmp/test.hkl",
                result_type="",
                number_of_hits=2,
                hit_rate=0.2,
                average_resolution=0,
                average_peaks_event=0,
            ),
        )
        ip_id = db.add_indexing_parameters(
            conn,
            DBIndexingParameters(
                id=None,
                tag=None,
                comment=None,
                software="dummy",
                software_version=None,
                command_line="dummy",
                parameters={},
                methods=[],
                geometry=None,
            ),
        )
        intp_id = db.add_integration_parameters(
            conn,
            DBIntegrationParameters(
                id=None,
                tag=None,
                comment=None,
                software="",
                software_version="",
                method="",
                center_boxes=None,
                overpredict=None,
                push_res=None,
                radius_inner=None,
                radius_outer=None,
                radius_middle=None,
            ),
        )
        _indexing_result_id = db.add_indexing_result(
            conn,
            DBIndexingResult(
                id=None,
                peak_search_parameters_id=psp_id,
                hit_finding_result_id=hit_finding_result_id,
                indexing_parameters_id=ip_id,
                integration_parameters_id=intp_id,
                num_indexed=1,
                num_crystals=1,
                tag="",
                comment="",
                result_filename="",
            ),
        )
        _data_source_2_id = db.add_data_source(
            conn,
            DBDataSource(id=None, run_id=RUN_ID, number_of_frames=10),
        )
        _second_hit_finding_result_id = db.add_hit_finding_result(
            conn,
            DBHitFindingResult(
                id=None,
                data_source_id=data_source_id,
                peak_search_parameters_id=psp_id,
                hit_finding_parameters_id=hfp_id,
                result_filename="/tmp/test.hkl",
                result_type="",
                number_of_hits=2,
                hit_rate=0.2,
                average_resolution=0,
                average_peaks_event=0,
            ),
        )

        analysis = db.retrieve_sample_based_analysis(conn)

        assert len(analysis) == 1
        assert analysis[0].sample_id == sample_id
        assert len(analysis[0].data_sources) == 2
