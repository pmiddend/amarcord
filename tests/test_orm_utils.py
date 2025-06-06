import datetime

import pytest

from amarcord.db import orm
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.orm_utils import update_orm_entity_has_attributo_value


def test_update_orm_entity_has_attributo_value_int() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeInt(),
        3,
    )
    assert result.integer_value == 3


def test_update_orm_entity_has_attributo_value_bool() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeBoolean(),
        v=True,
    )
    assert result.bool_value


def test_update_orm_entity_has_attributo_value_str() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeString(),
        "hehe",
    )
    assert result.string_value == "hehe"


def test_update_orm_entity_has_attributo_value_choice() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeChoice(values=["hehe"]),
        "hehe",
    )
    assert result.string_value == "hehe"


def test_update_orm_entity_has_attributo_value_decimal() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeDecimal(),
        3.5,
    )
    assert result.float_value == pytest.approx(3.5)


def test_update_orm_entity_has_attributo_value_datetime() -> None:
    result = orm.ChemicalHasAttributoValue(
        attributo_id=1,
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
    )
    t = datetime.datetime.now(datetime.timezone.utc)
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeDateTime(),
        t,
    )
    assert result.datetime_value == t


def test_update_orm_entity_has_attributo_value_chemical() -> None:
    result = orm.RunHasAttributoValue(
        attributo_id=AttributoId(1),
        integer_value=None,
        float_value=None,
        string_value=None,
        bool_value=None,
        datetime_value=None,
        list_value=None,
        chemical_value=None,
    )
    update_orm_entity_has_attributo_value(
        result,
        AttributoTypeChemical(),
        1,
    )
    assert result.chemical_value == 1
