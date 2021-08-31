import ast
import configparser
import datetime
import json
import re
from pathlib import Path
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List


# courtesy of https://lemire.me/blog/2008/12/17/fast-argmax-in-python/
def _argmin(array: Iterable[float]) -> int:
    array = list(array)
    return array.index(min(array))


def _get_pdb_chosen_by_dimple(dimple_log_path: Path) -> Path:
    """
    dimple is used to choose the closest matching PDB

    this function returns just the PDB file name, not a full path
    """

    # TJL 06 Jan 2021
    # I think this works and is much better
    # the old version (commented below) breaks if you have only one PDB input

    config = configparser.ConfigParser()
    config.read(dimple_log_path)

    pdbs = ast.literal_eval(config["workflow"]["pdb_files"])
    pdb_path = pdbs[0]  # they are in order

    # pdb_path = None
    #
    # with open(dimple_log_path, 'r') as f:
    #    watching = False
    #    for line in f.readlines():
    #
    #        if line.startswith('# PDBs in order of similarity (using the first one):'):
    #            watching = True
    #
    #        if line.startswith('#') and watching:
    #            g = re.search('(\S+\.pdb)', line)
    #            if g:
    #                pdb_path = g.groups()[0]
    #                break
    #
    # if pdb_path is None:
    #    raise IOError('could not find dimple-chosen reference PDB'
    #                  ' in: {}'.format(dimple_log_path))

    return Path(pdb_path)


def _stats_from_log(log_path: Path) -> List[float]:
    # from log: r-work r-free bonds angles b_min b_max b_ave
    p = r"end\:" + r"\s+(\d+\.\d+)" * 7
    with log_path.open("r") as f:
        g = re.search(p, f.read())

    if g is None:
        raise IOError("Could not parse: {}".format(log_path))

    return [float(e) for e in g.groups()]


def parse_dimple_output_directory(
    outdir: Path, metadata: str, check_paths_exist: bool = True
) -> Dict[str, Any]:
    # locate the initial pdb
    dimple1_log_path = outdir / "dimple.log"
    initial_pdb_path = _get_pdb_chosen_by_dimple(dimple1_log_path)

    # cycle through the pdbs produced by phenix, choose best r_free
    r_frees: List[float] = []
    for serial in [1, 2, 3]:
        log_path = outdir / "{}_{:03d}.log".format(metadata, serial)

        if log_path.is_file():
            try:
                _, r_free, _, _, _, _, _ = _stats_from_log(log_path)
                r_frees.append(r_free)
            except OSError:
                # this happens rarely when one phenix serial step fails
                # but the next one proceeds OK
                # print(e)
                r_frees.append(1.0)
        else:
            r_frees.append(1.0)

    # (remember serial is 1-indexed)
    best_serial = _argmin(r_frees) + 1
    if r_frees[best_serial - 1] is None:
        raise RuntimeError("cannot find valid phenix log for {}".format(metadata))

    log_path = outdir / "{}_{:03d}.log".format(metadata, best_serial)
    log_results = _stats_from_log(log_path)

    mtz_name = "{}_{:03d}.mtz".format(metadata, best_serial)
    mtz_path = outdir / mtz_name

    pdb_name = "{}_{:03d}.pdb".format(metadata, best_serial)
    pdb_path = outdir / pdb_name

    if check_paths_exist:
        for path in [
            outdir,
            initial_pdb_path,
            mtz_path,
            pdb_path,
            log_path,
            dimple1_log_path,
        ]:
            if not path.exists():
                raise IOError("{}/{} does not exist!" "".format(metadata, path))

    data_dict = {
        "analysis-time": datetime.datetime.fromtimestamp(
            mtz_path.stat().st_mtime
        ).isoformat(),
        "initial-pdb-path": str(initial_pdb_path),
        "final-pdb-path": str(pdb_path),
        "refinement-mtz-path": str(mtz_path),
        "rfree": log_results[1],
        "rwork": log_results[0],
        "rms-bond-length": log_results[2],
        "rms-bond-angle": log_results[3],
        "average-model-b": log_results[6],
    }

    return data_dict


def to_amarcord_output() -> None:
    # For now, assume we have results in the current directory and our metadata prefix is just "mymetadata"
    parsed = parse_dimple_output_directory(
        Path("."), "mymetadata", check_paths_exist=False
    )

    with Path("./amarcord-output.json").open("w") as f:
        parsed["type"] = "refinement"
        json.dump([parsed], f)


if __name__ == "__main__":
    to_amarcord_output()
