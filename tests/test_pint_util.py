from pint import UnitRegistry

from amarcord.pint_util import pint_quantity_to_attributo_type


def test_pint_quantity_to_attributo_type():
    result = pint_quantity_to_attributo_type(3 * UnitRegistry().meter)
    assert result.standard_unit
    assert result.suffix == "m"
