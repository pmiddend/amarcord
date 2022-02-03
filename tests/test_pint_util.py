from pint import UnitRegistry

from amarcord.pint_util import pint_quantity_to_attributo_type, valid_pint_unit


def test_pint_quantity_to_attributo_type():
    result = pint_quantity_to_attributo_type(3 * UnitRegistry().meter)
    assert result.standard_unit
    assert result.suffix == "m"


def test_valid_pint_unit():
    assert valid_pint_unit("m")
    assert valid_pint_unit("mm/s")
    assert not valid_pint_unit("foobar")
