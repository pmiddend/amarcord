import re
from dataclasses import dataclass
from typing import List

from enum import Enum


class CommandLineInputType(Enum):
    STRING = "string"
    DIFFRACTION_PATH = "diffraction.path"


@dataclass(frozen=True)
class CommandLineInput:
    name: str
    type_: CommandLineInputType


@dataclass(frozen=True)
class CommandLine:
    inputs: List[CommandLineInput]


def _parse_parameter(s: str) -> CommandLineInput:
    return CommandLineInput(
        type_=CommandLineInputType.STRING
        if s != CommandLineInputType.DIFFRACTION_PATH.value
        else CommandLineInputType.DIFFRACTION_PATH,
        name=s,
    )


def parse_command_line(s: str) -> CommandLine:
    return CommandLine([_parse_parameter(x) for x in re.findall(r"\${([^}]+)}", s)])
