from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from typing import cast

from amarcord.db.attributo_id import AttributoId
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBLinkedIndexingResult
from amarcord.db.table_classes import DBRun
from amarcord.db.table_classes import DBSampleAnalysisResult


def ds_hit_rate(k: DBLinkedDataSource) -> Optional[float]:
    if not k.hit_finding_results:
        return None
    return sorted(
        k.hit_finding_results,
        key=lambda x: cast(int, x.hit_finding_result.id),
        reverse=True,
    )[0].hit_finding_result.hit_rate


def ds_indexing_rate(k: DBLinkedDataSource) -> Optional[float]:
    hit_findings_with_indexings: List[DBLinkedHitFindingResult] = [
        hfr for hfr in k.hit_finding_results if hfr.indexing_results
    ]
    if not hit_findings_with_indexings:
        return None
    hit_findings_with_indexings.sort(
        key=lambda y: cast(int, y.hit_finding_result.id), reverse=True
    )
    latest_hfr = hit_findings_with_indexings[0]
    return (
        sorted(
            latest_hfr.indexing_results,
            key=lambda y: cast(int, y.indexing_result.id),
            reverse=True,
        )[0].indexing_result.num_indexed
        / k.data_source.number_of_frames
        * 100
    )


def sample_hit_rate(k: DBSampleAnalysisResult) -> Optional[float]:
    total_number_of_frames = sum(
        ip.data_source.number_of_frames for ip in k.data_sources
    )
    if total_number_of_frames == 0:
        return None

    return (
        sum(
            (ds_hit_rate(ip) if ds_hit_rate(ip) is not None else 0)  # type: ignore
            * ip.data_source.number_of_frames
            for ip in k.data_sources
        )
        / total_number_of_frames
    )


def sample_indexing_rate(k: DBSampleAnalysisResult) -> Optional[float]:
    total_number_of_frames = sum(
        ip.data_source.number_of_frames for ip in k.data_sources
    )
    if total_number_of_frames == 0:
        return None
    return (
        sum(
            (ds_indexing_rate(ip) if ds_indexing_rate(ip) is not None else 0)  # type: ignore
            * ip.data_source.number_of_frames
            for ip in k.data_sources
        )
        / total_number_of_frames
    )


def hfr_num_indexed(hfr: DBLinkedHitFindingResult) -> Optional[float]:
    return max(
        [ir.indexing_result.num_indexed for ir in hfr.indexing_results],
        default=None,
    )


def run_no_trains(r: DBRun) -> int:
    tir = r.attributi.select_int(AttributoId("trains_in_run"))
    return tir if tir is not None else 0


def sample_train_count(runs: Dict[int, DBRun], k: DBSampleAnalysisResult) -> int:
    return sum(
        run_no_trains(runs[ip.data_source.run_id])
        if ip.data_source.run_id in runs
        else 0
        for ip in k.data_sources
    )


def sample_duration(
    runs_with_train_count: Dict[int, int], k: DBSampleAnalysisResult
) -> Optional[int]:
    runs_for_sample = set(ip.data_source.run_id for ip in k.data_sources)
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
    return sum(r.data_source.number_of_frames for r in k.data_sources)


def data_source_duration(
    runs_with_train_count: Dict[int, int], k: DBLinkedDataSource
) -> int:
    duration_secs = runs_with_train_count.get(k.data_source.run_id, 0) / 10.0
    return int(duration_secs / 60.0)


TreeItem = Union[
    DBSampleAnalysisResult,
    DBLinkedDataSource,
    DBLinkedIndexingResult,
    DBLinkedHitFindingResult,
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
    ds: DBLinkedDataSource, runs_with_train_count: Dict[int, int]
) -> TreeNode:
    return TreeNode(
        f"Data source {ds.data_source.id} [run {ds.data_source.run_id}]",
        data_source_duration(runs_with_train_count, ds),
        ds.data_source.number_of_frames,
        ds.data_source.tag if ds.data_source.tag is not None else "",
        ds_hit_rate(ds),
        ds_indexing_rate(ds),
        ds,
        [
            build_hit_finding_tree(ds.data_source.number_of_frames, hfr)
            for hfr in ds.hit_finding_results
        ],
    )


def build_hit_finding_tree(
    total_number_of_frames: int, hfr: DBLinkedHitFindingResult
) -> TreeNode:
    indexed = hfr_num_indexed(hfr)
    return TreeNode(
        f"Hit Finding {hfr.hit_finding_result.id}",
        None,
        None,
        hfr.hit_finding_result.tag,
        hfr.hit_finding_result.hit_rate,
        indexed / total_number_of_frames * 100 if indexed is not None else None,
        hfr,
        [
            build_indexing_tree(total_number_of_frames, ir)
            for ir in hfr.indexing_results
        ],
    )


def build_indexing_tree(
    total_number_of_frames: int, ir: DBLinkedIndexingResult
) -> TreeNode:
    return TreeNode(
        f"Indexing {ir.indexing_result.id}",
        None,
        None,
        ir.indexing_result.tag,
        None,
        ir.indexing_result.num_indexed / total_number_of_frames * 100,
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
                for ds in k.data_sources
            ],
        )
        for k in analysis
    ]


def compute_hit_rate_per_run(dss: List[DBLinkedDataSource]) -> Dict[int, float]:
    hit_rate_per_run_accumulated: Dict[int, List[Tuple[int, float]]] = {}
    for ds in dss:
        run_id = ds.data_source.run_id
        if not ds.hit_finding_results:
            continue
        if run_id not in hit_rate_per_run_accumulated:
            hit_rate_per_run_accumulated[run_id] = []
        last_hfr = sorted(
            ds.hit_finding_results,
            key=lambda hfr: cast(int, hfr.hit_finding_result.id),
            reverse=True,
        )[0]
        hit_rate_per_run_accumulated[run_id].append(
            (ds.data_source.number_of_frames, last_hfr.hit_finding_result.hit_rate)
        )

    hit_rate_per_run: Dict[int, float] = {}
    for run_id, items in hit_rate_per_run_accumulated.items():
        hit_rate_per_run[run_id] = sum(x[0] * x[1] for x in items) / sum(
            x[0] for x in items
        )
    return hit_rate_per_run
