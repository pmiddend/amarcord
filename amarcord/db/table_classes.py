import datetime
from dataclasses import dataclass

from amarcord.db.attributi_map import AttributiMap
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId


@dataclass(frozen=True)
class DBFile:
    id: int | None
    description: str
    type_: str
    original_path: str | None
    file_name: str
    size_in_bytes: int
    contents: bytes | None


@dataclass(frozen=True)
class DBEvent:
    id: int
    created: datetime.datetime
    level: EventLogLevel
    source: str
    text: str
    files: list[DBFile]


@dataclass(frozen=True)
class DBChemical:
    id: int
    beamtime_id: BeamtimeId
    name: str
    responsible_person: str
    attributi: AttributiMap
    type_: ChemicalType
    files: list[DBFile]


@dataclass(frozen=True)
class DBRunOutput:
    id: RunInternalId
    external_id: RunExternalId
    beamtime_id: BeamtimeId
    attributi: AttributiMap
    started: datetime.datetime
    stopped: None | datetime.datetime
    experiment_type_id: int
    files: list[DBFile]


@dataclass(frozen=True)
class BeamtimeInput:
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: datetime.datetime
    end: datetime.datetime


@dataclass(frozen=True)
class BeamtimeOutput:
    id: BeamtimeId
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: datetime.datetime
    end: datetime.datetime
    chemical_names: None | list[str]
