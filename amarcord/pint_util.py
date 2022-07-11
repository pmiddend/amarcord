from pint import Quantity
from pint import UnitRegistry

from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeDecimal

registry = UnitRegistry()


def pint_quantity_to_attributo_type(x: Quantity) -> AttributoType | None:
    # see https://stackoverflow.com/questions/65681490/format-pint-unit-as-short-form-symbol
    # for an explanation
    suffix = format(x.units, "~")
    if suffix == "dimensionless":
        return AttributoTypeDecimal()
    return AttributoTypeDecimal(standard_unit=True, suffix=suffix)


def valid_pint_unit(s: str) -> bool:
    try:
        UnitRegistry()(s)
        return True
    except:
        return False
