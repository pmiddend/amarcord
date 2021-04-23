import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

from amarcord.modules.json import JSONDict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HarvestPeaksearch:
    method: str
    max_num_peaks: Optional[float]
    adc_threshold: Optional[float]
    minimum_snr: float
    min_pixel_count: int
    max_pixel_count: int
    min_res: int
    max_res: int
    bad_pixel_map_filename: Optional[Path]
    bad_pixel_map_hdf5_path: Optional[Path]
    local_bg_radius: Optional[float]
    min_peak_over_neighbor: Optional[float]
    min_snr_biggest_pix: Optional[float]
    min_snr_peak_pix: Optional[float]
    min_sig: Optional[float]
    min_squared_gradient: Optional[float]
    geometry: str


@dataclass(frozen=True)
class HarvestHitfinding:
    min_num_peaks: int


@dataclass(frozen=True)
class HarvestIndexing:
    parameters: JSONDict
    methods: List[str]


@dataclass(frozen=True)
class HarvestIntegration:
    method: Optional[str]
    center_boxes: Optional[bool]
    overpredict: Optional[bool]
    push_res: Optional[float]
    radius_inner: Optional[float]
    radius_middle: Optional[float]
    radius_outer: Optional[float]


@dataclass(frozen=True)
class HarvestJson:
    peaksearch: HarvestPeaksearch
    hitfinding: HarvestHitfinding
    indexing: Optional[HarvestIndexing]
    integration: Optional[HarvestIntegration]


def _get_string(j: Dict[str, Any], s: str) -> str:
    result = j[s]
    if not isinstance(result, str):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected string for "{s}", got {type(result)}'
        )
    return result


def _get_opt_string(j: Dict[str, Any], s: str) -> Optional[str]:
    result = j.get(s, None)
    if result is None:
        return None
    if not isinstance(result, str):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected string for "{s}", got {type(result)}'
        )
    return result


def _get_opt_path(j: Dict[str, Any], s: str) -> Optional[Path]:
    result = _get_opt_string(j, s)
    if result is None:
        return None
    return Path(result)


def _get_float(j: Dict[str, Any], s: str) -> float:
    result = j[s]
    if not isinstance(result, (float, int)):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected float for "{s}", got {type(result)}'
        )
    return result


def _get_int(j: Dict[str, Any], s: str) -> int:
    result = j[s]
    if not isinstance(result, int):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected int for "{s}", got {type(result)}'
        )
    return result


def _get_opt_int(j: Dict[str, Any], s: str) -> Optional[int]:
    result = j.get(s, None)
    if result is None:
        return None
    if not isinstance(result, int):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected int for "{s}", got {type(result)}'
        )
    return result


def _get_opt_bool(j: Dict[str, Any], s: str) -> Optional[bool]:
    result = j.get(s, None)
    if result is None:
        return None
    if not isinstance(result, bool):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected boolean for "{s}", got {type(result)}'
        )
    return result


def _get_str_list(j: Dict[str, Any], s: str) -> List[str]:
    result = j[s]
    if not isinstance(result, list):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected list of strings for "{s}", got {type(result)}'
        )
    if not result:
        return []
    if not isinstance(result[0], str):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected list of strings for "{s}", got list of {type(result[0])}'
        )
    return result


def _get_opt_float(j: Dict[str, Any], s: str) -> Optional[float]:
    result = j.get(s, None)
    if result is None:
        return None
    if not isinstance(result, (float, int)):
        raise ValueError(
            f'Invalid CrystFEL harvest JSON file: expected float for "{s}", got {type(result)}'
        )
    return result


def read_harvest_json(fn: Path) -> HarvestJson:
    with fn.open("r") as f:
        pf = json.load(f)
        ps = pf["peaksearch"]
        hf = pf["hitfinding"]
        indexing = pf["indexing"]
        integration = pf["integration"]
        return HarvestJson(
            HarvestPeaksearch(
                method=_get_string(ps, "method"),
                max_num_peaks=_get_opt_float(ps, "max_num_peaks"),
                adc_threshold=_get_opt_float(ps, "adc_threshold"),
                minimum_snr=_get_float(ps, "min_snr"),
                min_pixel_count=_get_int(ps, "min_pixel_count"),
                max_pixel_count=_get_int(ps, "max_pixel_count"),
                min_res=_get_int(ps, "min_res_px"),
                max_res=_get_int(ps, "max_res_px"),
                bad_pixel_map_filename=_get_opt_path(ps, "bad_pixel_map_filename"),
                bad_pixel_map_hdf5_path=_get_opt_path(ps, "bad_pixel_hdf5_path"),
                local_bg_radius=_get_opt_float(ps, "local_bg_radius_px"),
                min_peak_over_neighbor=_get_opt_float(ps, "min_peak_over_neighbor_adu"),
                min_snr_biggest_pix=_get_opt_float(ps, "min_snr_of_biggest_pixel"),
                min_snr_peak_pix=_get_opt_float(ps, "min_snr_of_peak_pixel"),
                min_sig=_get_opt_float(ps, "min_sig_adu"),
                min_squared_gradient=_get_opt_float(ps, "min_squared_gradient_adu2"),
                geometry="",
            ),
            HarvestHitfinding(min_num_peaks=_get_int(hf, "min_num_peaks")),
            HarvestIndexing(
                parameters=indexing,
                methods=_get_str_list(indexing, "methods"),
            )
            if indexing is not None
            else None,
            HarvestIntegration(
                method=_get_string(integration, "method"),
                center_boxes=_get_opt_bool(integration, "center_boxes"),
                overpredict=_get_opt_bool(integration, "overpredict"),
                push_res=_get_opt_float(integration, "push_res_invm"),
                radius_inner=_get_opt_float(integration, "radius_inner_px"),
                radius_middle=_get_opt_float(integration, "radius_middle_px"),
                radius_outer=_get_opt_float(integration, "radius_outer_px"),
            )
            if integration is not None
            else None,
        )


@dataclass(frozen=True)
class StreamMetadata:
    input_files: List[Path]
    version: Optional[str]
    geometry: str
    timestamp: Optional[float]
    num_hits: int
    hit_rate: float
    n_frames: int
    num_indexed: int
    num_crystals: int
    average_peaks_event: float
    average_resolution: float
    command_line: Optional[str]


def read_crystfel_streams(stream_list: Iterable[Path]) -> StreamMetadata:

    version: Optional[str] = None
    all_fns: List[Path] = []
    n_frames = 0
    n_hits = 0
    n_peaks = 0
    n_indexed = 0
    n_crystals = 0
    resolution: float = 0
    geom = ""
    in_geom = False
    have_geom = False
    command_line: Optional[str] = None
    timestamp: Optional[float] = None

    for fn in stream_list:
        timestamp = fn.stat().st_mtime
        logger.debug("reading stream file %s", fn)
        with fn.open("r") as f:
            while True:
                fline = f.readline()

                if not fline:
                    break

                if in_geom:
                    if fline.strip() == "----- End geometry file -----":
                        in_geom = False
                        have_geom = True
                    elif not have_geom:
                        geom += fline
                    continue

                fline = fline.strip()

                if fline.find("Generated by CrystFEL ") != -1 and not version:
                    version = fline.split(" ", 3)[3]
                    command_line = f.readline().strip()

                if fline.find("Image filename: ") != -1:
                    filename = Path(fline.split(": ", 1)[1])
                    n_frames += 1
                    if filename not in all_fns:
                        logger.debug("new filename: %s", filename)
                        all_fns.append(Path(filename))

                if fline == "hit = 1":
                    n_hits += 1

                if fline.find("peak_resolution") != -1:
                    resolution += float(fline.split(" ")[2]) * 1e9

                if fline.find("num_peaks = ") != -1:
                    n_peaks += int(fline.split(" = ", 1)[1])

                if fline.find("indexed_by = ") != -1:
                    indexed_by = fline.split(" = ", 1)[1]
                    if not indexed_by == "none":
                        n_indexed += 1

                if fline.find("Cell parameters") != -1:
                    n_crystals += 1

                if fline == "----- Begin geometry file -----":
                    in_geom = True

    return StreamMetadata(
        input_files=all_fns,
        version=version,
        geometry=geom,
        timestamp=timestamp,
        num_hits=n_hits,
        hit_rate=n_hits / n_frames,
        n_frames=n_frames,
        num_indexed=n_indexed,
        num_crystals=n_crystals,
        average_peaks_event=n_peaks / n_frames,
        average_resolution=resolution / n_frames,
        command_line=command_line,
    )
