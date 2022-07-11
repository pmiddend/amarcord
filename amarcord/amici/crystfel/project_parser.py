from dataclasses import dataclass
from pathlib import Path
from typing import Final

_INFO_LINE_DELIMITER: Final = "-----"


@dataclass(frozen=True)
class CrystfelProjectFile:
    info_lines: dict[str, str]
    # This might be slightly inaccurate, could be patterns next to the files
    file_lines: list[str]


def parse_crystfel_project_file(p: Path) -> CrystfelProjectFile:
    info_lines: dict[str, str] = {}
    file_lines: list[str] = []

    with p.open("r", encoding="utf-8") as f:
        previous_line: str | None = None
        for line_with_newline in f:
            line = line_with_newline.strip()

            if line == _INFO_LINE_DELIMITER:
                if previous_line == _INFO_LINE_DELIMITER:
                    break
                previous_line = line
                continue

            value_pair = line.split(" ", maxsplit=1)
            if len(value_pair) != 2:
                raise ValueError(f'{p}: line doesn\'t have two components: "{line}"')

            key, value = value_pair

            if key in info_lines:
                raise ValueError(f"{p}: key {key} is duplicate")

            info_lines[key] = value

        file_lines = [line.strip() for line in f]

    return CrystfelProjectFile(info_lines, file_lines)
