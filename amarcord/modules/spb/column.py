from typing import Final
from enum import Enum, auto


class Column(Enum):
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


def column_header(c: Column) -> str:
    d = {
        Column.RUN_ID: "Run",
        Column.STATUS: "Status",
        Column.SAMPLE: "Sample",
        Column.REPETITION_RATE: "Repetition Rate",
        Column.PULSE_ENERGY: "Pulse Energy",
        Column.TAGS: "Tags",
        Column.STARTED: "Started",
        Column.HIT_RATE: "Hit rate",
        Column.INDEXING_RATE: "Indexing rate",
        Column.COMMENTS: "Comments",
        Column.X_RAY_ENERGY: "X-ray energy",
        Column.INJECTOR_POSITION_Z_MM: "Injector position Z",
        Column.INJECTOR_FLOW_RATE: "Injector flow rate",
        Column.TRAINS: "Trains",
        Column.SAMPLE_DELIVERY_RATE: "Sample delivery rate",
        Column.DETECTOR_DISTANCE_MM: "Detector distance",
    }
    return d[c]


unplottable_columns: Final = set(
    {Column.RUN_ID, Column.STATUS, Column.SAMPLE, Column.TAGS, Column.COMMENTS}
)

default_visible_columns: Final = [
    Column.RUN_ID,
    Column.STATUS,
    Column.SAMPLE,
    Column.REPETITION_RATE,
    Column.TAGS,
    Column.HIT_RATE,
    Column.INDEXING_RATE,
    Column.COMMENTS,
]
