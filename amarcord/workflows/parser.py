import datetime
import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.amici.p11.refinement_result import RefinementResult
from amarcord.modules.json import JSONDict
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.xtal_util import find_space_group_index_by_name


class JSONChecker:
    def __init__(self, d: JSONDict, description: str) -> None:
        self.d = d
        self.description = description

    def optional_str(self, key: str) -> Optional[str]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, str):
            raise Exception(
                f'{self.description} result: value "{key}" not a string but {result}'
            )
        return result

    def optional_path(self, key: str) -> Optional[Path]:
        result = self.optional_str(key)
        return Path(result) if result is not None else None

    def optional_float(self, key: str) -> Optional[float]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, (float, int)):
            raise Exception(
                f'{self.description} result: value "{key}" not a float but {result}'
            )
        return float(result)

    def optional_int(self, key: str) -> Optional[int]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, int):
            raise Exception(
                f'{self.description} result: value "{key}" not an int but {result}'
            )
        return result

    def retrieve_safe(self, key: str) -> Any:
        result = self.d.get(key, None)
        if result is None:
            raise Exception(
                f'{self.description} result: couldn\'t get value "{key}", dict is: {self.d}'
            )
        return result

    def retrieve_safe_float(self, key: str) -> float:
        v = self.retrieve_safe(key)
        if not isinstance(v, (float, int)):
            raise Exception(
                f'{self.description} result: value "{key}" not a number: {v}'
            )
        return float(v)


def parse_refinement_result(
    base_path: Path, refinement: Dict[str, Any]
) -> RefinementResult:
    jsonc = JSONChecker(refinement, "refinement")
    return RefinementResult(
        analysis_time=datetime.datetime.fromisoformat(
            jsonc.retrieve_safe("analysis-time")
        ),
        folder_path=base_path,
        initial_pdb_path=jsonc.optional_path("initial-db-path"),
        final_pdb_path=jsonc.optional_path("final-db-path"),
        refinement_mtz_path=jsonc.optional_path("refinement-mtz-path"),
        comment=jsonc.optional_str("comment"),
        resolution_cut=jsonc.optional_float("resolution-cut"),
        rfree=jsonc.optional_float("rfree"),
        rwork=jsonc.optional_float("rwork"),
        rms_bond_length=jsonc.optional_float("rms-bond-length"),
        rms_bond_angle=jsonc.optional_float("rms-bond-angle"),
        num_blobs=jsonc.optional_int("rms-bond-angle"),
        average_model_b=jsonc.optional_float("average-model-b"),
    )


def parse_reduction_result(
    base_path: Path, reduction: Dict[str, Any]
) -> AnalysisResult:
    jsonc = JSONChecker(reduction, "reduction")

    space_group = jsonc.retrieve_safe("space-group")
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
        analysis_time=datetime.datetime.fromisoformat(
            jsonc.retrieve_safe("analysis-time")
        ),
        base_path=base_path,
        mtz_file=jsonc.optional_path("mtz-file"),
        method=ReductionMethod.OTHER,
        resolution_cc=jsonc.retrieve_safe_float("resolution-cc"),
        resolution_isigma=jsonc.retrieve_safe_float("resolution-isigma"),
        a=jsonc.retrieve_safe_float("a"),
        b=jsonc.retrieve_safe_float("b"),
        c=jsonc.retrieve_safe_float("c"),
        alpha=jsonc.retrieve_safe_float("alpha"),
        beta=jsonc.retrieve_safe_float("beta"),
        gamma=jsonc.retrieve_safe_float("gamma"),
        space_group=space_group_index,
        isigi=jsonc.optional_float("isigi"),
        rmeas=jsonc.optional_float("rmeas"),
        cchalf=jsonc.optional_float("cchalf"),
        rfactor=jsonc.optional_float("rfactor"),
        wilson_b=jsonc.optional_float("wilson-b"),
    )


def parse_workflow_result_file(
    fp: Path,
) -> Union[str, List[Union[AnalysisResult, RefinementResult]]]:
    with fp.open("r") as f:
        try:
            output_file_json = json.load(f)
        except Exception as e:
            return f"error parsing JSON result: {e}"

    if not isinstance(output_file_json, list):
        return (
            f"output file {fp} invalid JSON: not an array but {type(output_file_json)}"
        )

    results: List[Union[AnalysisResult, RefinementResult]] = []
    for result_idx, result in enumerate(output_file_json):
        if not isinstance(result, dict):
            return f"output file {fp} invalid JSON: index {result_idx} not a dictionary but: {result}"

        type_ = result.get("type", None)
        if type_ is None:
            return f"output file {fp} invalid JSON: index {result_idx} not a dictionary but: {result}"

        if type_ == "data_reduction":
            try:
                results.append(parse_reduction_result(fp.parent, result))
            except Exception as e:
                return f"error parsing reduction result: {e}"
        elif type_ == "refinement":
            try:
                results.append(parse_refinement_result(fp.parent, result))
            except Exception as e:
                return f"error parsing refinement result: {e}"
        else:
            return f'output file {fp} invalid JSON: index {result_idx}: invalid type "{type_}"'

    return results
