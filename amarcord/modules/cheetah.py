import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from typing import Final
from typing import List
from typing import Literal
from typing import Optional

CRAWLER_TXT: Final = "crawler.txt"

# noinspection SpellCheckingInspection
@dataclass(frozen=True)
class CheetahCrawlerConfigFile:
    xtcdir: Optional[Path]
    hdf5dir: Path
    hdf5filter: str
    geometry: Path
    process: Optional[Path]
    cheetahini: Optional[Path]
    cheetahtag: str
    runs_table_location: Path


# noinspection SpellCheckingInspection
@dataclass(frozen=True)
class CheetahCrawlerLine:
    run_id: int
    dataset: Optional[str]
    raw_data: Literal["Ready", "noAGIPD", "Incomplete"]
    cheetah_status: Literal[
        "unknown",
        "Not started",
        "Finished",
        "Not finished",
        "Submitted",
        "Terminated",
        "Error",
    ]
    hdf5_directory: Optional[Path]
    nprocessed: Optional[int]
    nhits: Optional[int]
    hit_rate: Optional[float]
    recipe: Optional[Path]
    calibration: Optional[Path]


# noinspection SpellCheckingInspection
@dataclass(frozen=True)
class CheetahRecipe:
    geometry_file: Path
    algorithm: int
    adc: int
    min_snr: int
    npeaks: int
    npeaks_max: int
    min_pix_count: int
    max_pix_count: int
    local_bg_radius: Optional[int]
    min_res: int
    max_res: int
    bad_pixel_map: Path


def cheetah_read_recipe(file_path: Path) -> CheetahRecipe:
    # I would love to just use the Python included configparser to parse the ini file
    # but it doesn't handle empty sections, so manual parsing it s...

    result: Dict[str, str] = {}
    with file_path.open("r") as f:
        for line in f:
            if not line.strip() or line and (line[0] == "#" or line[0] == "["):
                continue

            key, value = line.split("=")
            result[key] = value.strip()

    def int_or_none(r: str) -> Optional[int]:
        x = result.get(r, None)
        return int(x) if x is not None else x

    # noinspection SpellCheckingInspection
    return CheetahRecipe(
        geometry_file=Path(result["geometry"]),
        algorithm=int(result["hitfinderAlgorithm"]),
        adc=int(result["hitfinderADC"]),
        min_snr=int(result["hitfinderMinSNR"]),
        npeaks=int(result["hitfinderNpeaks"]),
        npeaks_max=int(result["hitfinderNpeaksMax"]),
        min_pix_count=int(result["hitfinderMinPixCount"]),
        max_pix_count=int(result["hitfinderMaxPixCount"]),
        local_bg_radius=int_or_none("hitfinderLocalBgRadius"),
        min_res=int(result["hitfinderMinRes"]),
        max_res=int(result["hitfinderMaxRes"]),
        bad_pixel_map=Path(result["badpixelmap"]),
    )


def cheetah_read_crawler_config_file(file_path: Path) -> CheetahCrawlerConfigFile:
    with file_path.open("r") as f:
        values: Dict[str, str] = {}
        for line in f:
            if not line.strip():
                continue
            key, value = line.split("=")
            values[key] = value.strip()
        return CheetahCrawlerConfigFile(
            xtcdir=Path(values["xtcdir"]) if "xtcdir" in values else None,
            hdf5dir=Path(values["hdf5dir"]),
            hdf5filter=values["hdf5filter"],
            geometry=Path(values["geometry"]),
            process=Path(values["process"]) if "process" in values else None,
            cheetahini=Path(values["cheetahini"]) if "cheetahini" in values else None,
            cheetahtag=values["cheetahtag"],
            runs_table_location=file_path.parent / CRAWLER_TXT,
        )


def cheetah_read_crawler_runs_table(file_path: Path) -> List[CheetahCrawlerLine]:
    def parse_invalid(x: str) -> Optional[str]:
        return None if x == "---" else x

    def parse_invalid_path(x: str) -> Optional[Path]:
        return None if x == "---" else Path(x)

    def parse_invalid_int(x: str) -> Optional[int]:
        return None if x == "---" else int(x)

    def parse_invalid_float(x: str) -> Optional[float]:
        return None if x == "---" else float(x)

    lines: List[CheetahCrawlerLine] = []
    with file_path.open() as f:
        for row in csv.DictReader(f):
            # noinspection PyTypeChecker,SpellCheckingInspection
            lines.append(
                CheetahCrawlerLine(
                    run_id=int(row["Run"]),
                    dataset=parse_invalid(row["Dataset"]),
                    raw_data=row["Rawdata"],  # type: ignore
                    cheetah_status=parse_invalid(row["Cheetah"]),  # type: ignore
                    hdf5_directory=parse_invalid_path(row["H5Directory"]),
                    nprocessed=parse_invalid_int(row["Nprocessed"]),
                    nhits=parse_invalid_int(row["Nhits"]),
                    hit_rate=parse_invalid_float(row["Hitrate%"]),
                    recipe=parse_invalid_path(row["Recipe"]),
                    calibration=parse_invalid_path(row["Calibration"]),
                )
            )
    return lines
