import datetime
import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.xtal_util import find_space_group_index_by_name


def parse_reduction_result(
    base_path: Path, reduction: Dict[str, Any]
) -> AnalysisResult:
    def optional_float(key: str) -> Optional[float]:
        result = reduction.get(key, None)
        if result is None:
            return None
        if not isinstance(result, (float, int)):
            raise Exception(f'reduction result: value "{key}" not a float but {result}')
        return float(result)

    def optional_path(key: str) -> Optional[Path]:
        result = reduction.get(key, None)
        return Path(result) if result is not None else None

    def retrieve_safe(key: str) -> Any:
        result = reduction.get(key, None)
        if result is None:
            raise Exception(
                f'reduction result: couldn\'t get value "{key}", dict is: {reduction}'
            )
        return result

    def retrieve_safe_float(key: str) -> float:
        v = retrieve_safe(key)
        if not isinstance(v, (float, int)):
            raise Exception(f'reduction result: value "{key}" not a number: {v}')
        return float(v)

    space_group = retrieve_safe("space-group")
    space_group_index: int

    if isinstance(space_group, int):
        space_group_index = space_group
    elif isinstance(space_group, str):
        space_group_index_raw = find_space_group_index_by_name(space_group)
        if space_group_index_raw is None:
            raise Exception(
                f'reduction result: value "space-group" not a valid space group: {space_group}'
            )
        space_group_index = space_group_index_raw
    else:
        raise Exception(
            f'reduction result: value "space-group" not a string or number: {space_group}'
        )

    return AnalysisResult(
        analysis_time=datetime.datetime.fromisoformat(retrieve_safe("analysis-time")),
        base_path=base_path,
        mtz_file=optional_path("mtz-file"),
        method=ReductionMethod.OTHER,
        resolution_cc=retrieve_safe_float("resolution-cc"),
        resolution_isigma=retrieve_safe_float("resolution-isigma"),
        a=retrieve_safe_float("a"),
        b=retrieve_safe_float("b"),
        c=retrieve_safe_float("c"),
        alpha=retrieve_safe_float("alpha"),
        beta=retrieve_safe_float("beta"),
        gamma=retrieve_safe_float("gamma"),
        space_group=space_group_index,
        isigi=optional_float("isigi"),
        rmeas=optional_float("rmeas"),
        cchalf=optional_float("cchalf"),
        rfactor=optional_float("rfactor"),
        wilson_b=optional_float("wilson-b"),
    )


def parse_workflow_result_file(fp: Path) -> Union[str, List[AnalysisResult]]:
    with fp.open("r") as f:
        try:
            output_file_json = json.load(f)
        except Exception as e:
            return f"error parsing JSON result: {e}"

    if not isinstance(output_file_json, list):
        return (
            f"output file {fp} invalid JSON: not an array but {type(output_file_json)}"
        )

    results: List[AnalysisResult] = []
    for result_idx, result in enumerate(output_file_json):
        if not isinstance(result, dict):
            return f"output file {fp} invalid JSON: index {result_idx} not a dictionary but: {result}"

        type_ = result.get("type", None)
        if type_ is None:
            return f"output file {fp} invalid JSON: index {result_idx} not a dictionary but: {result}"

        if type_ != "data_reduction":
            return f'output file {fp} invalid JSON: index {result_idx}: invalid type "{type_}"'

        try:
            results.append(parse_reduction_result(fp.parent, result))
        except Exception as e:
            return f"error parsing reduction result: {e}"

    return results
