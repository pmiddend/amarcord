# pylint: disable=use-tuple-over-list
import datetime
import logging
import pickle
from math import isclose
from pathlib import Path
from typing import Dict, Any, cast

import numpy.random
import pytest
import yaml

from amarcord.amici.xfel.karabo_bridge import (
    process_karabo_frame,
    KaraboAttributeDescription,
    KaraboValueLocator,
    KaraboInputType,
    KaraboProcessor,
    BridgeOutput,
    KaraboInternalId,
    frame_to_attributo_and_cache,
    KaraboValueByInternalId,
    AmarcordAttributoDescription,
    AttributoId,
    AmarcordAttributoProcessor,
    PlainAttribute,
    AttributoAccumulatorPerId,
    CoagulateList,
    CoagulateString,
    parse_coagulation_string,
    parse_karabo_attributes,
    KaraboConfigurationError,
    parse_karabo_attribute,
    parse_amarcord_attributi,
    parse_amarcord_attributo,
    parse_configuration,
    CONFIG_KARABO_ATTRIBUTES_KEY,
    CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES,
    CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY,
    KaraboBridgeConfiguration,
    CONFIG_KARABO_PROPOSAL,
    Karabo2,
    ingest_bridge_output,
    determine_attributo_type,
    ATTRIBUTO_ID_DARK_RUN_TYPE,
    accumulator_locators_for_config,
    persist_euxfel_run_result,
    process_trains,
)
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED, ATTRIBUTO_STOPPED
from amarcord.db.attributo_type import (
    AttributoTypeDecimal,
    AttributoTypeString,
    AttributoTypeList,
)
from amarcord.db.tables import create_tables_from_metadata

_SPECIAL_SOURCE = "amarcord"
_SPECIAL_RUN_NUMBER_KEY = "runNumber"
_SPECIAL_TRAINS_IN_RUN_KEY = "trainsInRun"
_SPECIAL_PROPOSAL_KEY = "proposal"
_SPECIAL_DARK_RUN_INDEX_KEY = "darkRunIndex"
_SPECIAL_DARK_RUN_TYPE_KEY = "darkRunType"

_STANDARD_SPECIAL_ATTRIBUTES = {
    "runNumber": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_RUN_NUMBER_KEY},
    "runStartedAt": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_RUN_NUMBER_KEY},
    "runFirstTrain": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_RUN_NUMBER_KEY},
    "runTrainsInRun": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_TRAINS_IN_RUN_KEY},
    "proposalId": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_PROPOSAL_KEY},
    "darkRunIndex": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_DARK_RUN_INDEX_KEY},
    "darkRunType": {"source": _SPECIAL_SOURCE, "key": _SPECIAL_DARK_RUN_TYPE_KEY},
}

logger = logging.getLogger(__name__)

ATTRIBUTO_DESCRIPTION = "attributo description"
ATTRIBUTO_ID1 = AttributoId("attributo-id")

INTERNAL_ID1 = KaraboInternalId("internal-id1")
INTERNAL_ID2 = KaraboInternalId("internal-id2")

DOOCS_INTENSITY_KEY = KaraboValueLocator(
    source="SPB_XTD9_XGM/XGM/DOOCS", subkey="data.intensitySa1TD"
)


def test_process_karabo_frame_one_list_attribute_arithmetic_mean() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_values = [1, 2.5, 2.5]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id
    # 2.0 being the mean of the intensity values
    assert frame.karabo_values_by_internal_id[INTERNAL_ID1] == 2.0


def test_process_karabo_frame_one_list_attribute_arithmetic_mean_floating_num() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    random_factor = (1.0 / numpy.random.random(1)[0]) ** 100.0
    intensity_values = [
        random_factor,
        random_factor * 2,
        random_factor * 4,
    ]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id


def test_process_karabo_frame_one_list_attribute_stdev() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_values = [1, 2]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id

    assert frame.karabo_values_by_internal_id[INTERNAL_ID1] == 0.5


def test_process_karabo_frame_one_list_attribute_one_element_stdev() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_values = [1]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id

    assert frame.karabo_values_by_internal_id[INTERNAL_ID1] == 0


def test_process_karabo_frame_one_list_attribute_take_last() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_values = [1, 2.5, 10.0]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id
    # 10 being the last value
    assert frame.karabo_values_by_internal_id[INTERNAL_ID1] == 10.0


def test_process_karabo_frame_one_list_float_attribute_take_last() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_value = 1.0
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: [intensity_value]}},
    )
    assert not frame.wrong_types
    assert not frame.not_found
    assert INTERNAL_ID1 in frame.karabo_values_by_internal_id
    assert frame.karabo_values_by_internal_id[INTERNAL_ID1] == 1.0


def test_process_karabo_frame_one_float_attribute_invalid_type() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            # definition here is float, but we'll give it a list of floats
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    intensity_values = [1.0]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert len(frame.wrong_types) == 1
    assert list(frame.wrong_types.values())[0].karabo_id == INTERNAL_ID1


def test_process_karabo_frame_one_float_attribute_not_found() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    frame = process_karabo_frame(
        attributes,
        # Source is empty, so we trigger the "not found" thingy
        {DOOCS_INTENSITY_KEY.source: {}},
    )
    assert len(frame.not_found) == 1


def test_process_karabo_frame_one_float_attribute_source_not_found() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    frame = process_karabo_frame(
        attributes,
        # Source is not given
        {},
    )
    assert len(frame.not_found) == 1


def test_process_karabo_frame_one_float_attribute_whole_source_not_found() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
            ignore=None,
        )
    ]
    frame = process_karabo_frame(
        attributes,
        # Source is not given
        {},
    )
    assert len(frame.not_found) == 1


def test_frame_to_attributo_and_cache_arithmetic_mean_plain_attribute() -> None:
    iterations = 200000
    # With floating numbers precision P=1e-16
    # the expected value over N operations with float numbers
    # should be close to the computed value by a factor P*sqrt(N)
    # Source https://fncbook.github.io/fnc/intro/floating-point.html
    relative_tolerance = 1.0e-16 * iterations**0.5

    random_factor = (1.0 / numpy.random.random(1)[0]) ** 100.0

    initial_value = random_factor
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_ARITHMETIC_MEAN,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    assert new_values[ATTRIBUTO_ID1] == initial_value

    next_value = 2
    while next_value < iterations:
        second_frame: KaraboValueByInternalId = {
            INTERNAL_ID1: next_value * random_factor
        }
        new_accumulator, new_values = frame_to_attributo_and_cache(
            second_frame, attributi, new_accumulator
        )
        next_value += 1

    expected = 0.5 * iterations * random_factor
    assert isclose(
        cast(float, new_values[ATTRIBUTO_ID1]),
        expected,
        rel_tol=relative_tolerance,
    )


def test_frame_to_attributo_and_cache_variance_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_VARIANCE,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    # None because the variance of one value is undefined
    assert new_values[ATTRIBUTO_ID1] is None

    # Second run. It's supposed to be the variance
    second_value = 11.0
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == 25.0


def test_frame_to_attributo_and_cache_stdev_from_stdev_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    # None because the variance of one value is undefined
    assert new_values[ATTRIBUTO_ID1] is None

    second_value = -1.0
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == 0.5**0.5


def test_frame_to_attributo_and_cache_stdev_from_values_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_STANDARD_DEVIATION,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    # None because the variance of one value is undefined
    assert new_values[ATTRIBUTO_ID1] is None

    second_value = 2.0
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == 0.5


def test_frame_to_attributo_and_cache_take_last_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    assert new_values[ATTRIBUTO_ID1] == initial_value

    # Second run. It's supposed to be "take last", so we expect the last value ot be returned
    second_value = 11.0
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == second_value


def test_frame_to_attributo_and_cache_take_last_plain_attribute_string() -> None:
    initial_value = "foo"
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            PlainAttribute(INTERNAL_ID1),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first value back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_accumulator
    assert ATTRIBUTO_ID1 in new_accumulator
    assert new_values[ATTRIBUTO_ID1] == initial_value

    # Second run. It's supposed to be "take last", so we expect the last value ot be returned
    second_value = "bar"
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == second_value


def test_frame_to_attributo_and_cache_take_last_coagulate_list() -> None:
    initial_value = 1.0
    initial_value2 = 100.0
    first_frame: KaraboValueByInternalId = {
        INTERNAL_ID1: initial_value,
        INTERNAL_ID2: initial_value2,
    }
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            CoagulateList([PlainAttribute(INTERNAL_ID1), PlainAttribute(INTERNAL_ID2)]),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first values back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_values[ATTRIBUTO_ID1] == [initial_value, initial_value2]

    # Second try, updated values now
    second_frame: KaraboValueByInternalId = {
        # Cleverly just flipped the values here
        INTERNAL_ID2: initial_value,
        INTERNAL_ID1: initial_value2,
    }
    _, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )
    assert new_values[ATTRIBUTO_ID1] == [initial_value2, initial_value]


def test_frame_to_attributo_and_cache_arithmetic_mean_coagulate_list() -> None:
    initial_value = 1.0
    initial_value2 = 100.0
    first_frame: KaraboValueByInternalId = {
        INTERNAL_ID1: initial_value,
        INTERNAL_ID2: initial_value2,
    }
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_ARITHMETIC_MEAN,
            CoagulateList([PlainAttribute(INTERNAL_ID1), PlainAttribute(INTERNAL_ID2)]),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first values back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_values[ATTRIBUTO_ID1] == [initial_value, initial_value2]

    # Second try, updated values now
    second_frame: KaraboValueByInternalId = {
        # Cleverly just flipped the values here
        INTERNAL_ID2: initial_value,
        INTERNAL_ID1: initial_value2,
    }
    _, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )
    # arithmetic mean of both separate (is the same because of flipped values though)
    assert new_values[ATTRIBUTO_ID1] == [50.5, 50.5]


def test_frame_to_attributo_and_cache_list_coagulate_string() -> None:
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: [1.0, 2.0]}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            CoagulateString(
                [
                    "foo",
                    PlainAttribute(INTERNAL_ID1),
                    "bar",
                ]
            ),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first values back
    _, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_values[ATTRIBUTO_ID1] == "foo[1.0, 2.0]bar"


def test_frame_to_attributo_and_cache_arithmetic_mean_coagulate_string() -> None:
    initial_value = 1.0
    initial_value2 = 100.0
    first_frame: KaraboValueByInternalId = {
        INTERNAL_ID1: initial_value,
        INTERNAL_ID2: initial_value2,
    }
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            ATTRIBUTO_DESCRIPTION,
            AmarcordAttributoProcessor.AMARCORD_PROCESSOR_ARITHMETIC_MEAN,
            CoagulateString(
                [
                    "foo",
                    PlainAttribute(INTERNAL_ID1),
                    "bar",
                    PlainAttribute(INTERNAL_ID2),
                    "baz",
                ]
            ),
        )
    ]
    old_accumulator: AttributoAccumulatorPerId = {}
    # Let it run once, we should simply get our first values back
    new_accumulator, new_values = frame_to_attributo_and_cache(
        first_frame, attributi, old_accumulator
    )
    assert new_values[ATTRIBUTO_ID1] == "foo1.00bar100.00baz"

    # Second try, updated values now
    second_frame: KaraboValueByInternalId = {
        # Cleverly just flipped the values here
        INTERNAL_ID2: initial_value,
        INTERNAL_ID1: initial_value2,
    }
    _, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )
    # arithmetic mean of both separate (is the same because of flipped values though)
    assert new_values[ATTRIBUTO_ID1] == "foo50.50bar50.50baz"


def test_parse_coagulation_string_no_matches() -> None:
    assert parse_coagulation_string("foo") == ["foo"]


def test_parse_coagulation_string_single_match() -> None:
    assert parse_coagulation_string("foo${bar}baz") == [
        "foo",
        KaraboInternalId("bar"),
        "baz",
    ]


def test_parse_coagulation_string_multiple_matches() -> None:
    assert parse_coagulation_string("foo${bar}baz${qux}quux") == [
        "foo",
        KaraboInternalId("bar"),
        "baz",
        KaraboInternalId("qux"),
        "quux",
    ]


def test_parse_coagulation_string_no_prefix_before_first_match() -> None:
    assert parse_coagulation_string("${bar}baz") == [
        KaraboInternalId("bar"),
        "baz",
    ]


def test_parse_coagulation_string_no_suffix_after_last_match() -> None:
    assert parse_coagulation_string("foo${bar}") == [
        "foo",
        KaraboInternalId("bar"),
    ]


def test_parse_karabo_attributes_not_a_dict() -> None:
    attributes = parse_karabo_attributes([])
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attributes_not_a_dict_per_source() -> None:
    attributes = parse_karabo_attributes({"source": [1, 2, 3]})
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attributes_not_a_dict_per_attribute() -> None:
    attributes = parse_karabo_attributes({"source": {"subkey": 1}})
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attributes_not_a_dict_per_attribute_subdict() -> None:
    attributes = parse_karabo_attributes({"source": {"subkey": [1]}})
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_id_missing() -> None:
    attributes = parse_karabo_attribute(
        {
            # "id": "foo",
            "input-type": "List[float]",
            "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_id_taken() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {KaraboInternalId("foo"): (KaraboValueLocator("source2", "subkey2"), 0)},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_id_wrong_type() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": 3,
            "input-type": "List[float]",
            "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_input_type_missing() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_input_type_wrong() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]kkkk",
            "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_processor_missing_non_scalar() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_processor_identity_non_scalar() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "identity",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert not isinstance(attributes, KaraboConfigurationError)


def test_parse_karabo_attribute_processor_missing_scalar() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "int",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboAttributeDescription)
    assert attributes.processor == KaraboProcessor.KARABO_PROCESSOR_IDENTITY


def test_parse_karabo_attribute_processor_wrong() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "take-lastkkk",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_unit_missing() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "is-si-unit": False,
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboAttributeDescription)


def test_parse_karabo_attribute_is_si_unit_not_boolean() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "is-si-unit": "random text",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    assert "is-si-unit should be a boolean, but is" in attributes.message
    logger.warning(attributes)


def test_parse_karabo_attribute_unknown_unit() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "unit": "nCC",
            "ignore": 1.0,
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    assert "got a unit in unit, but one we don't know" in attributes.message
    logger.warning(attributes)


def test_parse_karabo_attribute_unspcified_unit() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "ignore": 1.0,
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    assert "you didn't specify a unit" in attributes.message
    logger.warning(attributes)


def test_parse_karabo_attribute_non_numeric_ignore() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "unit": "nC",
            "ignore": "random useless text",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    assert "ignore only accepts numeric values" in attributes.message
    logger.warning(attributes)


def test_parse_karabo_attribute_ignore_on_non_numeric_attribute() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "str",
            "processor": "identity",
            "unit": "nC",
            "ignore": 1.0,
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    assert "ignore specified, but type is not numeric" in attributes.message
    logger.warning(attributes)


def test_parse_karabo_attribute_unit_wrong() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "take-last",
            "unit": "testtest",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_karabo_attribute_unit_wrong_but_nonstandard() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "List[float]",
            "processor": "list-take-last",
            "unit": "testtest",
            "is-si-unit": False,
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboAttributeDescription)
    assert attributes.unit == "testtest"
    assert not attributes.standard_unit


def test_parse_karabo_attribute_processor_wrong_combination() -> None:
    attributes = parse_karabo_attribute(
        {
            "id": "foo",
            "input-type": "float",
            "processor": "list-arithmetic-mean",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributi_not_a_dict() -> None:
    attributes = parse_amarcord_attributi(
        [],
        set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_duplicate_id() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi={AttributoId("a")},
        attributo_description={},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_description_id_not_string() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"description": 1},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_processor_wrong() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"processor": "foo"},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_plain_attribute_not_found() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"processor": "take-last", "plain-attribute": "foO"},
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_plain_attribute() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            "plain-attribute": "foo",
            "description": "test",
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, AmarcordAttributoDescription)
    assert attributes.attributo_id == "a"
    assert attributes.karabo_value_source == PlainAttribute(KaraboInternalId("foo"))
    assert attributes.description == "test"
    assert (
        attributes.processor == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST
    )


def test_parse_attributo_plain_attribute_wrong_processor() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value
            + "fff",
            "plain-attribute": "foo",
            "description": "test",
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_plain_attribute_both_coagulate_and_simple() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            "plain-attribute": "foo",
            "coagulate": ["foo"],
            "description": "test",
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_output_missing() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            "description": "test",
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_coagulate_list_invalid_id() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            # "bar" is not a valid ID
            "coagulate": ["foo", "bar"],
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_coagulate_list_no_components() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            # No components?
            "coagulate": [],
        },
        internal_ids={KaraboInternalId("foo"), KaraboInternalId("bar")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_coagulate_list() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            # "bar" is not a valid ID
            "coagulate": ["foo", "bar"],
        },
        internal_ids={KaraboInternalId("foo"), KaraboInternalId("bar")},
    )
    assert isinstance(attributes, AmarcordAttributoDescription)
    assert isinstance(attributes.karabo_value_source, CoagulateList)
    assert len(attributes.karabo_value_source.attributes) == 2
    assert attributes.karabo_value_source.attributes[0] == PlainAttribute(
        KaraboInternalId("foo")
    )
    assert attributes.karabo_value_source.attributes[1] == PlainAttribute(
        KaraboInternalId("bar")
    )


def test_parse_attributo_coagulate_string_invalid_id() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            # "foO" is not a valid ID
            "coagulate": "${foo} ${foO}",
        },
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_coagulate_string() -> None:
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={
            "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
            "coagulate": "${foo} ${bar}",
        },
        internal_ids={KaraboInternalId("foo"), KaraboInternalId("bar")},
    )
    assert isinstance(attributes, AmarcordAttributoDescription)
    assert isinstance(attributes.karabo_value_source, CoagulateString)
    assert len(attributes.karabo_value_source.value_sequence) == 3
    assert attributes.karabo_value_source.value_sequence[0] == PlainAttribute(
        KaraboInternalId("foo")
    )
    assert attributes.karabo_value_source.value_sequence[1] == " "
    assert attributes.karabo_value_source.value_sequence[2] == PlainAttribute(
        KaraboInternalId("bar")
    )


def test_parse_attributi() -> None:
    attributes = parse_amarcord_attributi(
        {
            "aid": {
                "processor": AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value,
                "coagulate": "${foo} ${bar}",
            }
        },
        {KaraboInternalId("foo"), KaraboInternalId("bar")},
    )
    assert isinstance(attributes, list)


def test_parse_configuration_not_a_dict() -> None:
    config = parse_configuration("random useless text")
    assert isinstance(config, KaraboConfigurationError)


def test_parse_configuration_empty() -> None:
    config = parse_configuration({})
    assert isinstance(config, KaraboConfigurationError)


def test_parse_configuration_without_proper_keys() -> None:
    config = parse_configuration({"useless key": "useless value"})
    assert isinstance(config, KaraboConfigurationError)
    logger.warning(config)


def test_parse_configuration_arithmetic_mean_of_list_karabo_attribute() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "List[float]",
                        "processor": "identity",
                        "is-si-unit": False,
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "pulse_energy_avg": {
                    "plain-attribute": "id1",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    logger.warning(config)


def test_parse_configuration_coagulation_of_karabo_list_attribute() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "List[float]",
                        "processor": "identity",
                        "is-si-unit": False,
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "pulse_energy_avg": {
                    "coagulate": ["id1"],
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    logger.warning(config)


def test_parse_configuration_stdev_coagulate_string() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "List[float]",
                        "processor": "list-standard-deviation",
                        "unit": "cm",
                    },
                    "subkey2": {
                        "id": "id2",
                        "input-type": "List[float]",
                        "processor": "list-arithmetic-mean",
                        "unit": "cm",
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "beam_pos": {
                    "coagulate": "${id1} ${id2}",
                    "processor": "propagated-standard-deviation",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    assert (
        "the type of all dependent Karabo attributes should be the list-standard-deviation"
        in config.message
    )
    logger.warning(config)


def test_parse_configuration_stdev_coagulate_list() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "List[float]",
                        "processor": "list-arithmetic-mean",
                        "unit": "cm",
                    },
                    "subkey2": {
                        "id": "id2",
                        "input-type": "List[float]",
                        "processor": "list-standard-deviation",
                        "unit": "cm",
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "beam_pos": {
                    "coagulate": ["id1", "id2"],
                    "processor": "propagated-standard-deviation",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    assert (
        "the type of all dependent Karabo attributes should be the list-standard-deviation"
        in config.message
    )
    logger.warning(config)


def test_parse_configuration_stdev_plain_attribute() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "float",
                        "unit": "cm",
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "beam_pos": {
                    "plain-attribute": "id1",
                    "processor": "propagated-standard-deviation",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    assert (
        "attributo beam_pos: the type of all dependent Karabo attributes should be the list-standard-deviation"
        in config.message
    )
    logger.warning(config)


def test_parse_configuration_stdev_plain_attribute_rev() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "List[float]",
                        "processor": "list-standard-deviation",
                        "unit": "cm",
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "beam_pos": {
                    "plain-attribute": "id1",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    assert "id1 can only be processed propagated-standard-deviation" in config.message
    logger.warning(config)


def test_parse_configuration_heterogeneous_list() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey1": {
                        "id": "id1",
                        "input-type": "int",
                    },
                    "subkey2": {
                        "id": "id2",
                        "input-type": "str",
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "pulse_energy_avg": {
                    "coagulate": ["id1", "id2"],
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboConfigurationError)
    logger.warning(config)


def test_parse_configuration() -> None:
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                    }
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                "pulse_energy_avg": {
                    "plain-attribute": "internal-id",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: 1234,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)


def test_karabo2_without_db() -> None:
    with (Path(__file__).parent / "karabo_online" / "config.yml").open("r") as f:
        configuration = parse_configuration(yaml.load(f, Loader=yaml.SafeLoader))
        assert isinstance(configuration, KaraboBridgeConfiguration)
        karabo2 = Karabo2(configuration)

    # Read first frame, this will have trains_in_run != 0, so nothing should happen
    with (
        Path(__file__).parent / "karabo_online" / "events" / "1035758364.pickle"
    ).open("rb") as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]

    frame_output1 = karabo2.process_frame(metadata, data)
    assert frame_output1 is None

    frame_output2 = karabo2.process_frame(metadata, data)
    assert frame_output2 is None

    # Now we change trains_in_run to 0, indicating a new run has started
    data[configuration.special_attributes.run_trains_in_run_key.source][
        # What the heck does pylint not understand here?!
        # pylint: disable=no-member
        configuration.special_attributes.run_trains_in_run_key.subkey
    ] = 0

    frame_output3 = karabo2.process_frame(metadata, data)
    assert frame_output3 is not None
    assert frame_output3.run_id == 53
    assert frame_output3.attributi_values[AttributoId("pulse_energy_avg")] > 500  # type: ignore
    # Variance still None because we need two values
    assert frame_output3.attributi_values[AttributoId("pulse_energy_variance")] is None
    assert frame_output3.attributi_values[AttributoId("beam_position")][0] == pytest.approx(0.8, 0.1)  # type: ignore
    assert frame_output3.attributi_values[AttributoId("beam_position")][1] == pytest.approx(0.07, 0.02)  # type: ignore
    assert frame_output3.attributi_values[ATTRIBUTO_STARTED] is not None
    assert ATTRIBUTO_STOPPED not in frame_output3.attributi_values

    # Same frame: expect a variance now
    frame_output4 = karabo2.process_frame(metadata, data)
    assert (
        frame_output4.attributi_values[AttributoId("pulse_energy_variance")] is not None  # type: ignore
    )

    # Now we change trains_in_run to a specific value again, indicating a new run has stopped
    # What the heck does pylint not understand here?!
    # pylint: disable=no-member
    data[configuration.special_attributes.run_trains_in_run_key.source][
        # What the heck does pylint not understand here?!
        # pylint: disable=no-member
        configuration.special_attributes.run_trains_in_run_key.subkey
    ] = 1

    frame_output5 = karabo2.process_frame(metadata, data)
    assert frame_output5.attributi_values[ATTRIBUTO_STOPPED] is not None  # type: ignore

    logger.warning(frame_output5.attributi_values)  # type: ignore


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def test_karabo2_with_db_frames_and_dummy_runs() -> None:
    RUN_ID = 53
    MISSING_RUNS = 3
    FIRST_EVENT = "1035758364.pickle"
    SECOND_EVENT = "1035758368.pickle"

    with (Path(__file__).parent / "karabo_online" / "config.yml").open("r") as f:
        configuration = parse_configuration(yaml.load(f, Loader=yaml.SafeLoader))
        assert isinstance(configuration, KaraboBridgeConfiguration)
        karabo2 = Karabo2(configuration)

    # Read first frame, this will have trains_in_run != 0, so nothing should happen
    with (Path(__file__).parent / "karabo_online" / "events" / FIRST_EVENT).open(
        "rb"
    ) as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        # pylint: disable=no-member
        data[configuration.special_attributes.dark_run_index_key.source][
            # pylint: disable=no-member
            configuration.special_attributes.dark_run_index_key.subkey
        ] = RUN_ID
        # pylint: disable=no-member
        data[configuration.special_attributes.dark_run_type_key.source][
            # pylint: disable=no-member
            configuration.special_attributes.dark_run_type_key.subkey
        ] = "testdark"

    karabo2.process_frame(metadata, data)

    # We change trains_in_run to 0, indicating a new run has started

    # pylint: disable=no-member
    data[configuration.special_attributes.run_trains_in_run_key.source][
        # pylint: disable=no-member
        configuration.special_attributes.run_trains_in_run_key.subkey
    ] = 0

    bridge_output = karabo2.process_frame(metadata, data)
    assert bridge_output is not None
    db = await _get_db()

    async with db.begin() as conn:
        await karabo2.create_missing_attributi(db, conn)
        await ingest_bridge_output(db, conn, bridge_output)
        runs = await db.retrieve_runs(
            conn, await db.retrieve_attributi(conn, associated_table=None)
        )
        assert len(runs) == 1
        assert runs[0].attributi.select_string(ATTRIBUTO_ID_DARK_RUN_TYPE) is not None

    # Read first frame, this will have trains_in_run != 0, so nothing should happen
    with (Path(__file__).parent / "karabo_online" / "events" / SECOND_EVENT).open(
        "rb"
    ) as handle:
        dataset_content = pickle.load(handle)
        data = dataset_content["data"]
        metadata = dataset_content["metadata"]
        data[configuration.special_attributes.run_number_key.source][
            # pylint: disable=no-member
            configuration.special_attributes.run_number_key.subkey
        ] = (RUN_ID + MISSING_RUNS + 1)

    new_bridge_output = karabo2.process_frame(metadata, data)
    assert new_bridge_output is not None

    async with db.begin() as conn:
        await karabo2.create_missing_attributi(db, conn)
        await ingest_bridge_output(db, conn, new_bridge_output)
        runs = await db.retrieve_runs(
            conn, await db.retrieve_attributi(conn, associated_table=None)
        )

        assert len(runs) == MISSING_RUNS + 2
        for run in runs:
            if run.id in [RUN_ID, RUN_ID + MISSING_RUNS + 1]:
                assert run.attributi.items()
            else:
                assert not run.attributi.items()


def test_determine_attributo_type_float_with_unit() -> None:
    assert determine_attributo_type(
        AmarcordAttributoDescription(
            AttributoId("a"),
            description=None,
            processor=AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            karabo_value_source=PlainAttribute(KaraboInternalId("a")),
        ),
        [
            KaraboAttributeDescription(
                id=KaraboInternalId("a"),
                locator=KaraboValueLocator("source", "subkey"),
                input_type=KaraboInputType.KARABO_TYPE_FLOAT,
                processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
                unit="ml",
                standard_unit=True,
                ignore=None,
            )
        ],
    ) == AttributoTypeDecimal(range=None, suffix="ml", standard_unit=True)


def test_determine_attributo_type_string() -> None:
    assert (
        determine_attributo_type(
            AmarcordAttributoDescription(
                AttributoId("a"),
                description=None,
                processor=AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
                karabo_value_source=CoagulateString(
                    [PlainAttribute(KaraboInternalId("a"))]
                ),
            ),
            [
                KaraboAttributeDescription(
                    id=KaraboInternalId("a"),
                    locator=KaraboValueLocator("source", "subkey"),
                    input_type=KaraboInputType.KARABO_TYPE_FLOAT,
                    processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
                    unit="ml",
                    standard_unit=True,
                    ignore=None,
                )
            ],
        )
        == AttributoTypeString()
    )


def test_determine_attributo_type_list_of_floats() -> None:
    assert determine_attributo_type(
        AmarcordAttributoDescription(
            AttributoId("a"),
            description=None,
            processor=AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST,
            karabo_value_source=CoagulateList(
                [
                    PlainAttribute(KaraboInternalId("a")),
                    PlainAttribute(KaraboInternalId("b")),
                ]
            ),
        ),
        [
            KaraboAttributeDescription(
                id=KaraboInternalId("a"),
                locator=KaraboValueLocator("source", "subkey"),
                input_type=KaraboInputType.KARABO_TYPE_FLOAT,
                processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
                unit="ml",
                standard_unit=True,
                ignore=None,
            ),
            KaraboAttributeDescription(
                id=KaraboInternalId("b"),
                locator=KaraboValueLocator("source", "subkey"),
                input_type=KaraboInputType.KARABO_TYPE_FLOAT,
                processor=KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
                unit="ml",
                standard_unit=True,
                ignore=None,
            ),
        ],
    ) == AttributoTypeList(
        sub_type=AttributoTypeDecimal(range=None, suffix="ml", standard_unit=True),
        min_length=None,
        max_length=None,
    )


def test_karabo2_simple_frame() -> None:
    proposal_id = 1337
    pulse_energy_avg = "pulse_energy_avg"
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                    }
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                pulse_energy_avg: {
                    "plain-attribute": "internal-id",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: proposal_id,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)
    karabo2 = Karabo2(config)
    initial_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to != 0 to indicate stopped run
            _SPECIAL_TRAINS_IN_RUN_KEY: 1,
        }
    }
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 1,
            }
        },
        initial_frame,
    )
    # None since we're in the middle of a stopped run
    assert result is None

    next_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to == 0 to indicate started run now
            _SPECIAL_TRAINS_IN_RUN_KEY: 0,
        },
        "source": {"subkey": [1.0, 2.0]},
    }

    # None since we're in the middle of a stopped run
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 2,
            }
        },
        next_frame,
    )
    assert result is not None
    assert result.attributi_values[AttributoId(pulse_energy_avg)] > 0.0  # type: ignore


def test_karabo2_ignore_value() -> None:
    proposal_id = 1337
    pulse_energy_avg = "pulse_energy_avg"
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                        "ignore": 1.0,
                    }
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                pulse_energy_avg: {
                    "plain-attribute": "internal-id",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: proposal_id,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)
    karabo2 = Karabo2(config)
    initial_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to != 0 to indicate stopped run
            _SPECIAL_TRAINS_IN_RUN_KEY: 1,
        }
    }
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 1,
            }
        },
        initial_frame,
    )
    # None since we're in the middle of a stopped run
    assert result is None

    next_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to == 0 to indicate started run now
            _SPECIAL_TRAINS_IN_RUN_KEY: 0,
        },
        "source": {"subkey": [1.0, 2.0]},
    }

    # None since we're in the middle of a stopped run
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 2,
            }
        },
        next_frame,
    )
    assert result is not None
    assert result.attributi_values[AttributoId(pulse_energy_avg)] == 2.0


def test_karabo2_missing_value() -> None:
    proposal_id = 1337
    pulse_energy_avg = "pulse_energy_avg"
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                        "ignore": 1.0,
                    }
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                pulse_energy_avg: {
                    "plain-attribute": "internal-id",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: proposal_id,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)
    karabo2 = Karabo2(config)
    initial_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to != 0 to indicate stopped run
            _SPECIAL_TRAINS_IN_RUN_KEY: 1,
        }
    }
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 1,
            }
        },
        initial_frame,
    )
    # None since we're in the middle of a stopped run
    assert result is None

    next_frame: Dict[str, Any] = {
        _SPECIAL_SOURCE: {
            _SPECIAL_PROPOSAL_KEY: proposal_id,
            _SPECIAL_RUN_NUMBER_KEY: 1,
            # Set to == 0 to indicate started run now
            _SPECIAL_TRAINS_IN_RUN_KEY: 0,
        },
        "source": {"subkey": [1.0]},
    }

    # None since we're in the middle of a stopped run
    result = karabo2.process_frame(
        {
            _SPECIAL_SOURCE: {
                "timestamp.tid": 2,
            }
        },
        next_frame,
    )
    assert result is not None
    assert AttributoId(pulse_energy_avg) not in result.attributi_values


def test_extra_data_locators_for_config() -> None:
    proposal_id = 1337
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                        "ignore": 1.0,
                    },
                    "subkey2": {
                        "id": "internal-id2",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                        "ignore": 1.0,
                    },
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {},
            CONFIG_KARABO_PROPOSAL: proposal_id,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)
    locators = accumulator_locators_for_config(config)
    assert "source" in locators
    assert len(locators["source"]) == 2
    assert "subkey" in locators["source"]
    assert "subkey2" in locators["source"]


async def test_persist_euxfel_run_result_new_run() -> None:
    RUN_ID = 1
    PROPOSAL_ID = 1001
    TEST_ATTRIBUTO = AttributoId("id")

    result = BridgeOutput(
        {TEST_ATTRIBUTO: "1"},
        RUN_ID,
        datetime.datetime.utcnow(),
        datetime.datetime.utcnow() + datetime.timedelta(seconds=10),
        PROPOSAL_ID,
        None,
        None,
    )
    result2 = BridgeOutput(
        {TEST_ATTRIBUTO: "2"},
        RUN_ID,
        datetime.datetime.utcnow(),
        datetime.datetime.utcnow() + datetime.timedelta(seconds=10),
        PROPOSAL_ID,
        None,
        None,
    )

    db = await _get_db()

    async with db.begin() as conn:
        await db.create_attributo(
            conn, "id", "desc", "grp", AssociatedTable.RUN, AttributoTypeString()
        )
        timestamps = [
            datetime.datetime.utcnow().timestamp() * 1e6,
            (datetime.datetime.utcnow().timestamp() + 10) * 1e6,
        ]
        await persist_euxfel_run_result(conn, db, result, RUN_ID, timestamps)

        retrieved_runs = await db.retrieve_runs(
            conn, await db.retrieve_attributi(conn, AssociatedTable.RUN)
        )

        assert len(retrieved_runs) == 1
        assert retrieved_runs[0].attributi.select_string(TEST_ATTRIBUTO) == "1"

        await persist_euxfel_run_result(conn, db, result2, RUN_ID, timestamps)

        retrieved_runs = await db.retrieve_runs(
            conn, await db.retrieve_attributi(conn, AssociatedTable.RUN)
        )

        assert len(retrieved_runs) == 1
        assert retrieved_runs[0].attributi.select_string(TEST_ATTRIBUTO) == "2"


def test_process_trains() -> None:
    proposal_id = 1337
    pulse_energy_avg = "pulse_energy_avg"
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "list-take-last",
                        "unit": "nC",
                    }
                }
            },
            CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES: _STANDARD_SPECIAL_ATTRIBUTES,
            CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY: {
                pulse_energy_avg: {
                    "plain-attribute": "internal-id",
                    "processor": "arithmetic-mean",
                },
            },
            CONFIG_KARABO_PROPOSAL: proposal_id,
        }
    )
    assert isinstance(config, KaraboBridgeConfiguration)
    karabo2 = Karabo2(config, first_train_is_start_of_run=True)
    trains = [
        (1001, {"source": {"subkey": [10.0, 10.1]}}),
        (1002, {"source": {"subkey": [10.2, 10.5]}}),
    ]
    result = process_trains(
        config, karabo2, run_id=1, train_ids=[1001, 1002], trains=trains
    )

    assert result is not None
    assert result.run_id == 1
    assert result.attributi_values[AttributoId(pulse_energy_avg)] == 10.3
