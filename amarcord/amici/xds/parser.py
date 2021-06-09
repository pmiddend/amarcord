from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union

import numpy as np
from pint import Quantity
from pint import UnitRegistry


@dataclass(frozen=True, eq=True)
class XDSCorrectLPFile:
    a: Quantity
    b: Quantity
    c: Quantity
    alpha: Quantity
    beta: Quantity
    gamma: Quantity
    space_group: int
    resolution_isigma: Optional[Quantity]
    resolution_cc: Optional[Quantity]
    isigi: Optional[float]
    rfactor: Optional[float]
    rmeas: Optional[float]
    cchalf: Optional[float]


def parse_correctlp(fn: Path, ureg: UnitRegistry) -> XDSCorrectLPFile:
    ios0 = 0.8
    cc_cutoff = 40
    r: List[float] = []
    ios: List[float] = []
    cc: List[float] = []

    result: Dict[str, Union[int, float]] = {}
    with fn.open("r") as f:
        reading_refl_stats = False
        reading_summary_stats = False

        for line in f:
            if line.startswith(" UNIT_CELL_CONSTANTS="):
                try:
                    (
                        result["a"],
                        result["b"],
                        result["c"],
                        result["alpha"],
                        result["beta"],
                        result["gamma"],
                    ) = [float(i) for i in line.split()[1:7]]
                except:
                    raise Exception(f"{fn}: unit cell constants are not float: {line}")

            elif line.startswith(" SPACE_GROUP_NUMBER="):
                try:
                    result["space_group"] = int(line.split()[1])
                except:
                    raise Exception(f"{fn}: space group is not an integer")

            elif line.startswith(" RESOLUTION RANGE  I/Sigma"):
                reading_refl_stats = True
                r = []
                ios = []

            elif line.startswith("   ------------") and reading_refl_stats:
                reading_refl_stats = False
                for i in range(len(r) - 1):
                    if ios[i] >= ios0 > ios[i + 1]:
                        resolution_isigma = np.round(  # type: ignore
                            np.interp(
                                [  # type: ignore
                                    ios0,
                                ],
                                [ios[i + 1], ios[i]],
                                [r[i + 1], r[i]],
                            )[0],
                            2,
                        )
                        if not isinstance(resolution_isigma, float):
                            raise Exception(
                                f"{fn}: resolution_isigma isn't float but {resolution_isigma}"
                            )
                        result["resolution_isigma"] = resolution_isigma

            elif reading_refl_stats:
                s = line.split()
                if len(s) == 9:
                    r.append(float(s[0]) / 2 + float(s[1]) / 2)
                    ios.append(float(s[2]))

            if line.startswith("   LIMIT     OBSERVED"):
                reading_summary_stats = True
                r = []
                cc = []

            elif line.startswith("    total") and reading_summary_stats:
                reading_summary_stats = False
                for i in range(len(r) - 1):
                    if cc[i] >= cc_cutoff > cc[i + 1]:
                        resolution_cc = np.round(  # type: ignore
                            np.interp(
                                [  # type: ignore
                                    cc_cutoff,
                                ],
                                [cc[i + 1], cc[i]],
                                [r[i + 1], r[i]],
                            )[0],
                            2,
                        )
                        if not isinstance(resolution_cc, float):
                            raise Exception(
                                f"{fn}: resolution_cc isn't float but {resolution_cc}"
                            )
                        result["resolution_cc"] = resolution_cc

                s = line.split()
                if len(s) == 14:
                    result["isigi"] = float(s[8])
                    result["rfactor"] = float(s[5].split("%")[0])
                    result["rmeas"] = float(s[9].split("%")[0])
                    result["cchalf"] = float(s[10].split("*")[0])

            elif reading_summary_stats:
                s = line.split()
                if len(s) == 14:
                    r.append(float(s[0]))
                    cc.append(float(s[10].split("*")[0]))

    def get_int_safely(result_dict: Dict[str, Union[int, float]], v: str) -> int:
        rv = result_dict.get(v)
        if rv is None:
            raise Exception(f"{fn}: couldn't find {v}")
        if not isinstance(rv, int):
            raise Exception(f"{fn}: {v} is not an int but {rv}")
        return rv

    def get_float_quantity(
        result_dict: Dict[str, Union[int, float]], v: str, quantity: Any
    ) -> Optional[Quantity]:
        rv = result_dict.get(v, None)
        if rv is None:
            return None
        if not isinstance(rv, float):
            raise Exception(f"{fn}: {v} is not float but {rv}")
        return rv * quantity

    def get_float(result_dict: Dict[str, Union[int, float]], v: str) -> Optional[float]:
        rv = result_dict.get(v, None)
        if rv is None:
            return None
        if not isinstance(rv, float):
            raise Exception(f"{fn}: {v} is not float but {rv}")
        return rv

    def get_float_safely(result_dict: Dict[str, Union[int, float]], v: str) -> float:
        rv = result_dict.get(v)
        if rv is None:
            raise Exception(f"{fn}: couldn't find {v}")
        if not isinstance(rv, float):
            raise Exception(f"{fn}: {v} is not float but {rv}")
        return rv

    return XDSCorrectLPFile(
        a=get_float_safely(result, "a") * ureg("angstrom"),
        b=get_float_safely(result, "b") * ureg("angstrom"),
        c=get_float_safely(result, "c") * ureg("angstrom"),
        alpha=get_float_safely(result, "alpha") * ureg("degree"),
        beta=get_float_safely(result, "beta") * ureg("degree"),
        gamma=get_float_safely(result, "gamma") * ureg("degree"),
        space_group=get_int_safely(result, "space_group"),
        resolution_isigma=get_float_quantity(
            result, "resolution_isigma", ureg("angstrom")
        ),
        resolution_cc=get_float_quantity(result, "resolution_cc", ureg("angstrom")),
        isigi=get_float(result, "isigi"),
        rfactor=get_float(result, "rfactor"),
        rmeas=get_float(result, "rmeas"),
        cchalf=get_float(result, "cchalf"),
    )


@dataclass(frozen=True, eq=True)
class XDSResultsFile:
    wilson_b: float


def parse_resultsfile(fn: Path):
    wilson_b: Optional[float] = None
    with fn.open() as f:
        for line in f:
            if line.startswith("    B(Wilson)"):
                try:
                    wilson_b = float(line.split()[-1])
                except:
                    raise Exception(f"{fn}: B(Wilson) is not a float but {line}")

    if wilson_b is None:
        raise Exception(f"{fn}: Didn't find wilson_b in the file")

    return XDSResultsFile(wilson_b)
