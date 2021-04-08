from typing import List
from typing import Optional

from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBLinkedIndexingResult
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.table_classes import DBSampleAnalysisResult
from amarcord.modules.spb.analysis_tree import build_analysis_tree
from amarcord.modules.spb.analysis_tree import ds_hit_rate
from amarcord.modules.spb.analysis_tree import ds_indexing_rate


def hit_finding_result_mock(
    hf_id: int,
    ds_id: int,
    hit_rate: float,
    number_of_hits: int,
    indexing_results: Optional[List[DBLinkedIndexingResult]] = None,
) -> DBLinkedHitFindingResult:
    return DBLinkedHitFindingResult(
        peak_search_parameters=DBPeakSearchParameters(
            id=1,
            method="method",
            software="software",
        ),
        hit_finding_parameters=DBHitFindingParameters(
            id=1, min_peaks=1, tag=None, comment=None, software=""
        ),
        hit_finding_result=DBHitFindingResult(
            id=hf_id,
            peak_search_parameters_id=1,
            hit_finding_parameters_id=1,
            data_source_id=ds_id,
            result_filename="",
            result_type="",
            average_peaks_event=0,
            average_resolution=0,
            number_of_hits=number_of_hits,
            hit_rate=hit_rate,
        ),
        indexing_results=[] if indexing_results is None else indexing_results,
    )


def test_analysis_tree_no_data_sources() -> None:
    tree = build_analysis_tree(
        [DBSampleAnalysisResult(sample_id=1, sample_name="a", data_sources=[])],
        {},
    )
    assert len(tree) == 1
    assert not tree[0].children
    assert tree[0].indexing_rate is None
    assert tree[0].hit_rate is None
    assert tree[0].number_of_frames == 0
    assert tree[0].duration_minutes == 0


def test_datasource_hit_rate() -> None:
    """
    Hit rate is calculated by looking at the latest hit finding result (highest ID)
    """
    hr = ds_hit_rate(
        DBLinkedDataSource(
            DBDataSource(
                id=1,
                run_id=1,
                number_of_frames=300,
            ),
            hit_finding_results=[
                hit_finding_result_mock(1, 1, 30, 300),
                hit_finding_result_mock(2, 1, 5, 10),
            ],
        )
    )
    hr_reversed = ds_hit_rate(
        DBLinkedDataSource(
            DBDataSource(
                id=1,
                run_id=1,
                number_of_frames=300,
            ),
            hit_finding_results=[
                hit_finding_result_mock(2, 1, 5, 10),
                hit_finding_result_mock(1, 1, 30, 300),
            ],
        )
    )

    assert hr == 5
    assert hr_reversed == 5


# noinspection PyTypeChecker
def test_datasource_indexing_rate_simple() -> None:
    """
    Indexing rate is calculated by looking at the latest hit finding result and the latest indexing result (highest ID).

    Here, we test the case that we have two hit finding results, but only one has an indexing result.
    """
    ir = ds_indexing_rate(
        DBLinkedDataSource(
            DBDataSource(
                id=1,
                run_id=1,
                number_of_frames=300,
            ),
            hit_finding_results=[
                hit_finding_result_mock(
                    1,
                    1,
                    30,
                    300,
                    [
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=1,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=10,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                    ],
                ),
                hit_finding_result_mock(2, 1, 5, 10),
            ],
        )
    )

    assert ir == 10 / 300 * 100


# noinspection PyTypeChecker
def test_datasource_indexing_rate_multiple() -> None:
    """
    With just one hit finding result, we expect the latest indexing is taken, no matter how high the rate is.
    """
    ir = ds_indexing_rate(
        DBLinkedDataSource(
            DBDataSource(
                id=1,
                run_id=1,
                number_of_frames=300,
            ),
            hit_finding_results=[
                hit_finding_result_mock(
                    1,
                    1,
                    30,
                    300,
                    [
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=1,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=10,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=2,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=1,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                    ],
                ),
                hit_finding_result_mock(2, 1, 5, 10),
            ],
        )
    )

    assert ir == 1 / 300 * 100


# noinspection PyTypeChecker
def test_datasource_indexing_rate_multiple_hit_findings() -> None:
    """
    In this test, we have two hit finding results and two indexing results. We expect to take the "latest" path
    and thus to take the latest hit finding and inside, the latest indexing.
    """
    ds = DBDataSource(
        id=1,
        run_id=1,
        number_of_frames=300,
    )
    ir = ds_indexing_rate(
        DBLinkedDataSource(
            ds,
            hit_finding_results=[
                hit_finding_result_mock(
                    1,
                    1,
                    30,
                    300,
                    [
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=1,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=10,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=2,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=1,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                    ],
                ),
                hit_finding_result_mock(
                    2,
                    1,
                    5,
                    10,
                    [
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=1,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=200,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                        DBLinkedIndexingResult(
                            indexing_result=DBIndexingResult(
                                id=2,
                                hit_finding_result_id=1,
                                peak_search_parameters_id=1,
                                indexing_parameters_id=1,
                                integration_parameters_id=1,
                                num_indexed=300,
                                num_crystals=1,
                                tag=None,
                                comment=None,
                                result_filename="",
                            ),
                            peak_search_parameters=None,  # type: ignore
                            indexing_parameters=None,  # type: ignore
                            integration_parameters=None,  # type: ignore
                        ),
                    ],
                ),
            ],
        )
    )

    assert ir == 100
