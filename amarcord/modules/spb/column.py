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
