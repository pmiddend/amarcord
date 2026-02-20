import pytest

from amarcord.db import orm
from amarcord.db.attributi import decimal_attributi_match
from amarcord.db.attributi import run_matches_dataset
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeString

BOOL_ATTRIBUTO_ID = AttributoId(1)
STRING_ATTRIBUTO_ID = AttributoId(2)

RUN_ATTRIBUTI_STRING_THERE_BOOL_MISSING: dict[AttributoId, orm.RunHasAttributoValue] = {
    STRING_ATTRIBUTO_ID: orm.RunHasAttributoValue(
        attributo_id=STRING_ATTRIBUTO_ID,
        string_value="foo",
        integer_value=None,
        bool_value=None,
        float_value=None,
        datetime_value=None,
        chemical_value=None,
        list_value=None,
    ),
}

DS_ATTRIBUTI_STRING_THERE_BOOL_FALSE: dict[
    AttributoId,
    orm.DataSetHasAttributoValue,
] = {
    STRING_ATTRIBUTO_ID: orm.DataSetHasAttributoValue(
        attributo_id=STRING_ATTRIBUTO_ID,
        string_value="foo",
        integer_value=None,
        bool_value=None,
        float_value=None,
        datetime_value=None,
        chemical_value=None,
        list_value=None,
    ),
    BOOL_ATTRIBUTO_ID: orm.DataSetHasAttributoValue(
        attributo_id=BOOL_ATTRIBUTO_ID,
        bool_value=False,
        integer_value=None,
        string_value=None,
        float_value=None,
        datetime_value=None,
        chemical_value=None,
        list_value=None,
    ),
}


@pytest.mark.parametrize(
    ("run_attributi", "data_set_attributi", "outcome"),
    [
        (
            RUN_ATTRIBUTI_STRING_THERE_BOOL_MISSING,
            DS_ATTRIBUTI_STRING_THERE_BOOL_FALSE,
            True,
        ),
    ],
)
def test_run_matches_data_set(
    run_attributi: dict[AttributoId, orm.RunHasAttributoValue],
    data_set_attributi: dict[AttributoId, orm.DataSetHasAttributoValue],
    outcome: bool,
) -> None:
    attributo_types = {
        BOOL_ATTRIBUTO_ID: AttributoTypeBoolean(),
        STRING_ATTRIBUTO_ID: AttributoTypeString(),
    }
    assert (
        run_matches_dataset(
            attributo_types,
            run_attributi=run_attributi,
            data_set_attributi=data_set_attributi,
        )
        == outcome
    )


@pytest.mark.parametrize(
    ("run_value", "data_set_value", "tolerance", "tolerance_is_absolute", "matches"),
    [
        (
            201.0,
            200.0,
            0.1,
            False,
            True,
        ),
        (
            220.0,
            200.0,
            0.1,
            False,
            True,
        ),
        (
            225.0,
            200.0,
            0.1,
            False,
            False,
        ),
        (
            210.0,
            200.0,
            13,
            True,
            True,
        ),
        (
            214.0,
            200.0,
            13,
            True,
            False,
        ),
        (
            200.0,
            200.0,
            None,
            True,
            True,
        ),
        (
            200.0,
            201.0,
            None,
            True,
            False,
        ),
    ],
)
def test_decimal_attributi_match(
    run_value: float | None,
    data_set_value: float | None,
    tolerance: float | None,
    tolerance_is_absolute: bool,
    matches: bool,
) -> None:
    assert (
        decimal_attributi_match(
            AttributoTypeDecimal(
                range=None,
                suffix=None,
                standard_unit=False,
                tolerance=tolerance,
                tolerance_is_absolute=tolerance_is_absolute,
            ),
            run_value,
            data_set_value,
        )
        == matches
    )
