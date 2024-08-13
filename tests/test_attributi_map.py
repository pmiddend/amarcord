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
    )
}

DS_ATTRIBUTI_STRING_THERE_BOOL_FALSE: dict[
    AttributoId, orm.DataSetHasAttributoValue
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
    "run_attributi, data_set_attributi, outcome",
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


# def test_run_matches_dataset_bool_and_string() -> None:
#     first_id = AttributoId(1)
#     second_id = AttributoId(2)
#     run_attributi_no_boolean = {
#         second_id: orm.RunHasAttributoValue(
#             run_id=1, attributo_id=second_id, string_value="foo"
#         )
#     }
#     run_attributi_boolean_false = {
#         second_id: orm.RunHasAttributoValue(
#             run_id=1, attributo_id=second_id, bool_value=False
#         )
#     }
#     run_attributi_boolean_true = {
#         second_id: orm.RunHasAttributoValue(
#             run_id=1, attributo_id=second_id, string_value="foo"
#         ),
#         first_id: orm.RunHasAttributoValue(
#             run_id=1, attributo_id=first_id, bool_value=True
#         ),
#     }

#     data_set_attributi_boolean_false = {
#         second_id: orm.DataSetHasAttributoValue(
#             data_set_id=1, attributo_id=second_id, string_value="foo"
#         ),
#         first_id: orm.DataSetHasAttributoValue(
#             # Boolean has to be false (or missing!)
#             data_set_id=1,
#             attributo_id=first_id,
#             bool_value=False,
#         ),
#     }
#     data_set_attributi_boolean_true = {
#         second_id: orm.DataSetHasAttributoValue(
#             data_set_id=1, attributo_id=second_id, string_value="foo"
#         ),
#         first_id: orm.DataSetHasAttributoValue(
#             # Boolean has to be false (or missing!)
#             data_set_id=1,
#             attributo_id=first_id,
#             bool_value=True,
#         ),
#     }

#     attributo_types = {
#         first_id: AttributoTypeBoolean(),
#         second_id: AttributoTypeString(),
#     }
#     assert run_matches_dataset(
#         attributo_types,
#         run_attributi=run_attributi_no_boolean,
#         data_set_attributi=data_set_attributi_boolean_false,
#     )
#     assert run_matches_dataset(
#         attributo_types,
#         run_attributi=run_attributi_boolean_false,
#         data_set_attributi=data_set_attributi_boolean_false,
#     )
#     assert not run_matches_dataset(
#         attributo_types,
#         run_attributi=run_attributi_boolean_true,
#         data_set_attributi=data_set_attributi_boolean_false,
#     )
#     assert not run_matches_dataset(
#         attributo_types,
#         run_attributi=run_attributi_boolean_false,
#         data_set_attributi=data_set_attributi_boolean_true,
#     )
#     assert not run_matches_dataset(
#         attributo_types,
#         run_attributi=run_attributi_no_boolean,
#         data_set_attributi=data_set_attributi_boolean_true,
#     )


@pytest.mark.parametrize(
    "run_value, data_set_value, tolerance, tolerance_is_absolute, matches",
    [
        (
            201.0,
            200.0,
            0.1,
            False,
            True,
        ),
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
