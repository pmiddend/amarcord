from pathlib import Path
from typing import Any, Type, Tuple

from amarcord.amici.crystfel.project_parser import CrystfelProjectFile


def config_file_from_crystfel_project(
    p: CrystfelProjectFile,
) -> dict[str, Any]:
    crystfel_lines = p.info_lines

    result: dict[str, Any] = {}

    geom = crystfel_lines.get("geom", None)
    if geom is None:
        raise ValueError('missing "geom" line in CrystFEL file')

    if not Path(geom).is_file():
        raise ValueError(f"geometry file {geom} doesn't exist or is not a file")

    result["geom"] = {"file_path": crystfel_lines}

    crystfel_to_xwiz: dict[str, Tuple[str, Type]] = {
        "peak_search_params.method": ("peak_method", str),
        "peak_search_params.threshold": ("peak_threshold", float),
        "peak_search_params.min_snr": ("peak_snr", float),
        "peak_search_params.min_pix_count": ("peak_min_px", int),
        "peak_search_params.max_pix_count": ("peak_max_px", int),
        "indexing.methods": ("index_method", str),
        "peak_search_params.local_bg_radius": ("local_bg_radius", int),
        "peak_search_params.max_res": ("max_res", int),
        "indexing.min_peaks": ("min_peaks", int),
    }

    proc_coarse: dict[str, Any] = {}
    result["proc_coarse"] = proc_coarse
    for crystfel, (xwiz, xwiz_type) in crystfel_to_xwiz.items():
        crystfel_value = crystfel_lines.get(crystfel, None)
        if crystfel_value is None:
            continue
        if xwiz_type == int:
            proc_coarse[xwiz] = int(crystfel_value)
        elif xwiz_type == float:
            proc_coarse[xwiz] = float(crystfel_value)
        else:
            assert xwiz_type == str
            proc_coarse[xwiz] = crystfel_value

    cell_file = crystfel_lines.get("indexing.cell_file", None)
    if cell_file is not None:
        assert isinstance(cell_file, str)

        if not Path(cell_file).is_file():
            raise ValueError(f'cell file "{cell_file}" does not exist or is not a file')
        result["unit_cell"] = {"file": cell_file}

    return result
