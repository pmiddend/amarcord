import re
from dataclasses import dataclass
from enum import Enum
from typing import List


class CommandLineInputType(Enum):
    STRING = "string"
    DIFFRACTION_PATH = "diffraction.path"
    REDUCTION_MTZ_PATH = "reduction.mtz_path"
    REDUCTION_FOLDER_PATH = "reduction.folder_path"


@dataclass(frozen=True)
class CommandLineInput:
    name: str
    type_: CommandLineInputType


@dataclass(frozen=True)
class CommandLine:
    inputs: List[CommandLineInput]

    def contains_reductions(self) -> bool:
        return any(
            f.type_
            in (
                CommandLineInputType.REDUCTION_FOLDER_PATH,
                CommandLineInputType.REDUCTION_MTZ_PATH,
            )
            for f in self.inputs
        )


def _parse_parameter(s: str) -> CommandLineInput:
    for e in CommandLineInputType:
        if e.value == s:
            return CommandLineInput(type_=e, name=s)
    return CommandLineInput(type_=CommandLineInputType.STRING, name=s)


def parse_command_line(s: str) -> CommandLine:
    return CommandLine([_parse_parameter(x) for x in re.findall(r"\${([^}]+)}", s)])
