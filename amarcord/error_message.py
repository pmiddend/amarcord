from dataclasses import dataclass


@dataclass(frozen=True)
class ErrorMessage:
    message: str
