from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from amarcord.db.attributo_id import AttributoId
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBSampleAnalysisResult


def ds_hit_rate(k: DBDataSource) -> Optional[float]:
    if not k.hit_finding_results:
        return None
    return sorted(k.hit_finding_results, key=lambda x: cast(int, x.id), reverse=True)[
        0
    ].hit_rate


def ds_indexing_rate(k: DBDataSource) -> Optional[float]:
    hit_findings_with_indexings = [
        hfr for hfr in k.hit_finding_results if hfr.indexing_results
    ]
    if not hit_findings_with_indexings:
        return None
    hit_findings_with_indexings.sort(key=lambda y: cast(int, y.id), reverse=True)
    latest_hfr = hit_findings_with_indexings[0]
    return (
        sorted(
            latest_hfr.indexing_results,
            key=lambda y: cast(int, y.id),
            reverse=True,
        )[0].num_indexed
        / k.number_of_frames
        * 100
    )


def sample_hit_rate(k: DBSampleAnalysisResult) -> Optional[float]:
    total_number_of_frames = sum(ip.number_of_frames for ip in k.indexing_paths)
    if total_number_of_frames == 0:
        return None

    return (
        sum(
            (ds_hit_rate(ip) if ds_hit_rate(ip) is not None else 0)  # type: ignore
            * ip.number_of_frames
            for ip in k.indexing_paths
        )
        / total_number_of_frames
    )


def sample_indexing_rate(k: DBSampleAnalysisResult) -> Optional[float]:
    total_number_of_frames = sum(ip.number_of_frames for ip in k.indexing_paths)
    if total_number_of_frames == 0:
        return None
    return (
        sum(
            (ds_indexing_rate(ip) if ds_indexing_rate(ip) is not None else 0)  # type: ignore
            * ip.number_of_frames
            for ip in k.indexing_paths
        )
        / total_number_of_frames
    )


def hfr_num_indexed(hfr: DBHitFindingResult) -> Optional[float]:
    return max(
        [ir.num_indexed for ir in hfr.indexing_results],
        default=None,
    )


def run_no_trains(r: DBRun) -> int:
    first_train = r.attributi.select_value(AttributoId("first_train"))
    last_train = r.attributi.select_value(AttributoId("last_train"))
    assert first_train is None or isinstance(
        first_train, int
    ), f'Got {type(first_train)} instead of integer for "first_train"'
    assert last_train is None or isinstance(
        last_train, int
    ), f'Got {type(last_train)} instead of integer for "last_train"'
    return (
        last_train - first_train
        if first_train is not None and last_train is not None
        else 0
    )


def sample_train_count(runs: Dict[int, DBRun], k: DBSampleAnalysisResult) -> int:
    return sum(
        run_no_trains(runs[ip.run_id]) if ip.run_id in runs else 0
        for ip in k.indexing_paths
    )


def sample_duration(
    runs_with_train_count: Dict[int, int], k: DBSampleAnalysisResult
) -> Optional[int]:
    runs_for_sample = set(ip.run_id for ip in k.indexing_paths)
    duration_secs = (
        sum(
            runs_with_train_count[run_id]
            for run_id in runs_for_sample
            if run_id in runs_with_train_count
        )
        / 10.0
    )
    return int(duration_secs / 60.0)


def sample_number_of_frames(k: DBSampleAnalysisResult) -> int:
    return sum(r.number_of_frames for r in k.indexing_paths)


def data_source_duration(runs_with_train_count: Dict[int, int], k: DBDataSource) -> int:
    duration_secs = runs_with_train_count.get(k.run_id, 0) / 10.0
    return int(duration_secs / 60.0)


TreeItem = Union[
    DBSampleAnalysisResult, DBDataSource, DBIndexingResult, DBHitFindingResult
]


@dataclass(frozen=True)
class TreeNode:
    description: str
    duration_minutes: Optional[int]
    number_of_frames: Optional[int]
    tag: Optional[str]
    hit_rate: Optional[float]
    indexing_rate: Optional[float]
    value: TreeItem
    children: List["TreeNode"]


def build_data_source_tree(
    ds: DBDataSource, runs_with_train_count: Dict[int, int]
) -> TreeNode:
    return TreeNode(
        f"Data source {ds.id} [run {ds.run_id}]",
        data_source_duration(runs_with_train_count, ds),
        ds.number_of_frames,
        ds.tag if ds.tag is not None else "",
        ds_hit_rate(ds),
        ds_indexing_rate(ds),
        ds,
        [
            build_hit_finding_tree(ds.number_of_frames, hfr)
            for hfr in ds.hit_finding_results
        ],
    )


def build_hit_finding_tree(
    total_number_of_frames: int, hfr: DBHitFindingResult
) -> TreeNode:
    indexed = hfr_num_indexed(hfr)
    return TreeNode(
        f"Hit Finding {hfr.id}",
        None,
        None,
        hfr.tag,
        hfr.hit_rate,
        indexed / total_number_of_frames * 100 if indexed is not None else None,
        hfr,
        [
            build_indexing_tree(total_number_of_frames, ir)
            for ir in hfr.indexing_results
        ],
    )


def build_indexing_tree(total_number_of_frames: int, ir: DBIndexingResult) -> TreeNode:
    return TreeNode(
        f"Indexing {ir.id}",
        None,
        None,
        ir.tag,
        None,
        ir.num_indexed / total_number_of_frames * 100,
        ir,
        [],
    )


def runs_to_number_of_trains(runs: List[DBRun]) -> Dict[int, int]:
    return {r.id: run_no_trains(r) for r in runs}


def build_analysis_tree(
    analysis: List[DBSampleAnalysisResult], runs_with_train_count: Dict[int, int]
) -> List[TreeNode]:
    return [
        TreeNode(
            f"Sample “{k.sample_name}”",
            sample_duration(runs_with_train_count, k),
            sample_number_of_frames(k),
            "",
            sample_hit_rate(k),
            sample_indexing_rate(k),
            k,
            [
                build_data_source_tree(ds, runs_with_train_count)
                for ds in k.indexing_paths
            ],
        )
        for k in analysis
    ]
