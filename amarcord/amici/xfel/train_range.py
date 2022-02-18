from dataclasses import dataclass


@dataclass
class TrainRange:
    train_begin_inclusive: int
    train_end_inclusive: int
