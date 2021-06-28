import datetime
import xml.etree.ElementTree as ET
from pathlib import Path

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.amici.p11.db import ReductionMethod


def parse_staraniso_directory(p: Path) -> AnalysisResult:
    alldata_unique_xml = p / "staraniso_alldata-unique.xml"
    mtz_file = p / "staraniso_alldata-unique.mtz"

    if not mtz_file.is_file():
        raise Exception(f"{p}: couldn't find mtz file {mtz_file}")

    tree = ET.parse(alldata_unique_xml)
    root = tree.getroot()

    autoproc_elements = root.findall("AutoProc")

    if len(autoproc_elements) != 1:
        raise Exception(
            f"{p}: tried to find the only <AutoProc> node in the file, got {(len(autoproc_elements))}"
        )

    autoproc = autoproc_elements[0]

    def find_or_raise(el: ET.Element, x: str) -> str:
        elements = el.findall(x)
        if len(elements) != 1:
            raise Exception(
                f"{p}: tried to find the only <{x}> node in the file, got {(len(elements))}"
            )
        if not elements[0].text:
            raise Exception(f"{p}: element <{x}> has no text content!")
        return elements[0].text

    space_group = find_or_raise(autoproc, "spaceGroup")
    cell_a = float(find_or_raise(autoproc, "refinedCell_a"))
    cell_b = float(find_or_raise(autoproc, "refinedCell_b"))
    cell_c = float(find_or_raise(autoproc, "refinedCell_c"))
    cell_alpha = float(find_or_raise(autoproc, "refinedCell_alpha"))
    cell_beta = float(find_or_raise(autoproc, "refinedCell_beta"))
    cell_gamma = float(find_or_raise(autoproc, "refinedCell_gamma"))

    scaling_dict = {
        "resolutionLimitHigh": "resolution_isigma",
        "rMerge": "rfactor",
        "rMeasAllIPlusIMinus": "rmeas",
        "meanIOverSigI": "isigi",
        "ccHalf": "cchalf",
    }

    scaling_stats = {
        find_or_raise(scaling_stat, "scalingStatisticsType"): {
            amarcord_type: float(find_or_raise(scaling_stat, xml_type))
            for xml_type, amarcord_type in scaling_dict.items()
        }
        for scaling_stat in root.iter("AutoProcScalingStatistics")
    }

    overall_stat = scaling_stats.get("overall", None)
    if overall_stat is None:
        raise Exception(
            f"{p}: didn't get the overall stats in XML, just {scaling_stats.keys()}"
        )

    return AnalysisResult(
        analysis_time=datetime.datetime.fromtimestamp(
            p.stat().st_mtime, tz=datetime.timezone.utc
        ),
        base_path=p,
        mtz_file=mtz_file,
        method=ReductionMethod.STARANISO,
        resolution_cc=scaling_stats.get("outerShell", {}).get("cchalf", None),
        resolution_isigma=overall_stat["resolution_isigma"],
        a=cell_a,
        b=cell_b,
        c=cell_c,
        alpha=cell_alpha,
        beta=cell_beta,
        gamma=cell_gamma,
        space_group=space_group,
        isigi=overall_stat["isigi"],
        rmeas=overall_stat["rmeas"],
        cchalf=overall_stat["cchalf"],
        rfactor=overall_stat["rfactor"],
        wilson_b=None,
    )
