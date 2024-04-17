import pytest
import structlog

from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import parse_cell_description

logger = structlog.stdlib.get_logger(__name__)


@pytest.mark.parametrize(
    "input_string, cell_file",
    [
        (
            "monoclinic P c (3.4 5.6 7.8) (40.0 50.0 60.0)",
            CrystFELCellFile(
                lattice_type="monoclinic",
                centering="P",
                unique_axis="c",
                a=3.4,
                b=5.6,
                c=7.8,
                alpha=40.0,
                beta=50.0,
                gamma=60.0,
            ),
        ),
        (
            "monoclinic P ? (3.4 5.6 7.8) (40.0 50.0 60.0)",
            CrystFELCellFile(
                lattice_type="monoclinic",
                centering="P",
                unique_axis=None,
                a=3.4,
                b=5.6,
                c=7.8,
                alpha=40.0,
                beta=50.0,
                gamma=60.0,
            ),
        ),
        ("monoclini P ? (3.4 5.6 7.8) (40.0 50.0 60.0)", None),
    ],
)
def test_parse_cell_description(
    input_string: str, cell_file: None | CrystFELCellFile
) -> None:
    assert parse_cell_description(input_string) == cell_file
