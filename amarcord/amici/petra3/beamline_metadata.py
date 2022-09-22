import json
from pathlib import Path

from pydantic import BaseModel


class OnlineAnalysis(BaseModel):
    reservedNodes: list[str]
    slurmPartition: str | None
    slurmReservation: str | None
    sshPrivateKeyPath: Path | None
    sshPublicKeyPath: Path | None
    userAccount: str | None


class BeamlineMetadata(BaseModel):
    beamtimeId: str
    onlineAnalysis: OnlineAnalysis


def beamtime_directory(beamtime_id: str, beamline: str, year: int) -> Path:
    return Path(f"/asap3/petra3/gpfs/{beamline}/{year}/data/{beamtime_id}")


def locate_beamtime_metadata(
    beamtime_id: str, beamline: str, year: int
) -> None | BeamlineMetadata:
    result = (
        beamtime_directory(beamtime_id, beamline, year)
        / f"beamline-metadata-{beamtime_id}.json"
    )
    if result.is_file():
        return parse_beamline_metadata(result)
    return None


def _make_paths_absolute(p: Path, m: BeamlineMetadata) -> BeamlineMetadata:
    result = m.copy()
    if result.onlineAnalysis.sshPrivateKeyPath is not None:
        result.onlineAnalysis.sshPrivateKeyPath = (
            p / result.onlineAnalysis.sshPrivateKeyPath
        )
    if result.onlineAnalysis.sshPublicKeyPath is not None:
        result.onlineAnalysis.sshPublicKeyPath = (
            p / result.onlineAnalysis.sshPublicKeyPath
        )
    return result


def parse_beamline_metadata(p: Path) -> BeamlineMetadata:
    with p.open("r") as f:
        return _make_paths_absolute(p.parent, BeamlineMetadata(**json.load(f)))
