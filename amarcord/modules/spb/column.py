from typing import Any, Final
from enum import Enum, auto


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
        return ", ".join(r)
    assert isinstance(r, (int, float, str, bool))
    return str(v)
