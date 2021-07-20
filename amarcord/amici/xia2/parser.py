import json
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.util import path_mtime


def traverse_dict_path(d: Dict[str, Any], p: List[str]) -> Optional[Any]:
    result = d
    for s in p:
        result = result.get(s, {})
    return result


def parse_xia2_directory(p: Path) -> AnalysisResult:
    xia2_file = p / "xia2.json"

    if not xia2_file.is_file():
        raise Exception(f"{p}: no {xia2_file.name} present")

    try:
        with xia2_file.open("r") as f:
            json_result = json.load(f)
    except Exception as e:
        raise Exception(f"{p}: couldn't read {xia2_file}: {e}")

    if not isinstance(json_result, dict):
        raise Exception(f"{p}: xia2 file is not a dictionary but {(type(json_result))}")

    mtz_file = next(iter(p.glob("DataFiles/*_free.mtz")), None)

    if mtz_file is None:
        raise Exception(f"{p}: no mtz file with glob *_free.mtz found")

    scaler_path = ["_crystals", "DEFAULT", "_scaler"]
    scaler_node = traverse_dict_path(json_result, scaler_path)

    if scaler_node is None:
        raise Exception(
            f"{p}: tried to find the scaler, but didn't find the necessary nodes on path: {scaler_path}"
        )

    cell = scaler_node.get("_scalr_cell", [])
    if not isinstance(cell, list) or len(cell) != 6:
        raise Exception(f"{p}: cell array doesn't have length 6: {cell}")
    a = cell[0]
    b = cell[1]
    c = cell[2]
    alpha = cell[3]
    beta = cell[4]
    gamma = cell[5]

    def retrieve_singular_dict(root: Dict[str, Any], s: str) -> Dict[str, Any]:
        result_ = root.get(s, None)
        if result_ is None:
            raise Exception(f'{p}: found no "{s}" node')
        if not isinstance(result_, dict):
            raise Exception(f'{p}: "{s}" node is not a dict but {type(result_)}')
        if len(result_.values()) != 1:
            raise Exception(f'{p}: found {len(result_.values())} values for "{s}" node')
        return next(iter(result_.values()))

    integrater = retrieve_singular_dict(scaler_node, "_scalr_integraters")
    statistics = retrieve_singular_dict(scaler_node, "_scalr_statistics")

    return AnalysisResult(
        analysis_time=path_mtime(xia2_file),
        base_path=p,
        mtz_file=mtz_file,
        method=ReductionMethod.DIALS_DIALS,
        resolution_cc=statistics["CC half"][2],
        resolution_isigma=statistics["dI/s(dI)"][0],
        a=a,
        b=b,
        c=c,
        alpha=alpha,
        beta=beta,
        gamma=gamma,
        space_group=integrater["_intgr_spacegroup_number"],
        isigi=statistics["I/sigma"][0],
        rmeas=statistics["Rmeas(I)"][0],
        cchalf=statistics["CC half"][0],
        rfactor=statistics["Rmerge(I)"][0],
        wilson_b=statistics["Wilson B factor"][0],
    )
