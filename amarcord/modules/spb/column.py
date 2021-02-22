import datetime
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, Final, List, Optional, Tuple, TypeVar, Union


class RunProperty(Enum):
    RUN_ID = auto()
    STATUS = auto()
    SAMPLE = auto()
    STARTED = auto()
    REPETITION_RATE = auto()
    TAGS = auto()
    PULSE_ENERGY = auto()
    HIT_RATE = auto()
    INDEXING_RATE = auto()
    COMMENTS = auto()
    X_RAY_ENERGY = auto()
    INJECTOR_POSITION_Z_MM = auto()
    DETECTOR_DISTANCE_MM = auto()
    INJECTOR_FLOW_RATE = auto()
    TRAINS = auto()
    SAMPLE_DELIVERY_RATE = auto()


def run_property_name(c: RunProperty) -> str:
    d = {
        RunProperty.RUN_ID: "Run",
        RunProperty.STATUS: "Status",
        RunProperty.SAMPLE: "Sample",
        RunProperty.REPETITION_RATE: "Repetition Rate",
        RunProperty.PULSE_ENERGY: "Pulse Energy",
        RunProperty.TAGS: "Tags",
        RunProperty.STARTED: "Started",
        RunProperty.HIT_RATE: "Hit rate",
        RunProperty.INDEXING_RATE: "Indexing rate",
        RunProperty.COMMENTS: "Comments",
        RunProperty.X_RAY_ENERGY: "X-ray energy",
        RunProperty.INJECTOR_POSITION_Z_MM: "Injector position Z",
        RunProperty.INJECTOR_FLOW_RATE: "Injector flow rate",
        RunProperty.TRAINS: "Trains",
        RunProperty.SAMPLE_DELIVERY_RATE: "Sample delivery rate",
        RunProperty.DETECTOR_DISTANCE_MM: "Detector distance",
    }
    return d[c]


unplottable_properties: Final = {
    RunProperty.RUN_ID,
    RunProperty.STATUS,
    RunProperty.SAMPLE,
    RunProperty.TAGS,
    RunProperty.COMMENTS,
}

default_visible_properties: Final = [
    RunProperty.RUN_ID,
    RunProperty.STATUS,
    RunProperty.SAMPLE,
    RunProperty.REPETITION_RATE,
    RunProperty.TAGS,
    RunProperty.HIT_RATE,
    RunProperty.INDEXING_RATE,
    RunProperty.COMMENTS,
]


def run_property_to_string(r: RunProperty, v: Any) -> str:
    if r == RunProperty.TAGS:
        assert isinstance(v, list)
        return ", ".join(v)
    if isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if not isinstance(v, (int, float, str, bool)):
        raise Exception(f"run property {r} has invalid type {type(v)}")
    return str(v)


@dataclass(frozen=True)
class PropertyInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class PropertyDouble:
    range: Optional[Tuple[float, float]] = None
    nonNegative: bool = False


@dataclass(frozen=True)
class PropertyTags:
    pass


@dataclass(frozen=True)
class PropertySample:
    pass


@dataclass(frozen=True)
class PropertyDateTime:
    pass


T = TypeVar("T")


@dataclass(frozen=True)
class PropertyChoice:
    values: List[Tuple[str, T]]


PropertyType = Union[
    PropertyInt,
    PropertyChoice,
    PropertyDouble,
    PropertyTags,
    PropertySample,
    PropertyDateTime,
]

run_property_type: Final[Dict[RunProperty, PropertyType]] = {
    RunProperty.RUN_ID: PropertyInt(),
    RunProperty.TAGS: PropertyTags(),
    RunProperty.STATUS: PropertyChoice(
        values=[("finished", "finished"), ("running", "running")]
    ),
    RunProperty.SAMPLE: PropertySample(),
    RunProperty.REPETITION_RATE: PropertyDouble(),
    RunProperty.DETECTOR_DISTANCE_MM: PropertyDouble(),
    RunProperty.HIT_RATE: PropertyDouble(range=(0.0, 100.0)),
    RunProperty.INDEXING_RATE: PropertyDouble(range=(0.0, 100.0)),
    RunProperty.INJECTOR_FLOW_RATE: PropertyDouble(nonNegative=True),
    RunProperty.PULSE_ENERGY: PropertyDouble(nonNegative=True),
    RunProperty.INJECTOR_POSITION_Z_MM: PropertyDouble(),
    RunProperty.SAMPLE_DELIVERY_RATE: PropertyDouble(nonNegative=True),
    RunProperty.STARTED: PropertyDateTime(),
    RunProperty.TRAINS: PropertyInt(nonNegative=True),
    RunProperty.X_RAY_ENERGY: PropertyDouble(nonNegative=True),
}
