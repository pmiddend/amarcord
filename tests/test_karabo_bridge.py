import logging
import pickle
from pathlib import Path

import yaml

from amarcord.amici.xfel.karabo_bridge import (
    process_karabo_frame,
    KaraboAttributeDescription,
    KaraboValueLocator,
    KaraboInputType,
    KaraboProcessor,
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
)
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED, ATTRIBUTO_STOPPED
from amarcord.db.attributo_type import (
    AttributoTypeDecimal,
    AttributoTypeString,
    AttributoTypeList,
)
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata

_STANDARD_SPECIAL_ATTRIBUTES = {
    "runNumber": {"source": "a", "key": "bar"},
    "runStartedAt": {"source": "a", "key": "bar"},
    "runFirstTrain": {"source": "a", "key": "bar"},
    "runTrainsInRun": {"source": "a", "key": "bar"},
    "proposalId": {"source": "a", "key": "bar"},
}

logger = logging.getLogger(__name__)

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


def test_process_karabo_frame_one_list_attribute_take_last() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_LIST_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
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


def test_process_karabo_frame_one_float_attribute_take_last() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
        )
    ]
    intensity_value = 1.0
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_value}},
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
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
        )
    ]
    intensity_values = [1.0]
    frame = process_karabo_frame(
        attributes,
        {DOOCS_INTENSITY_KEY.source: {DOOCS_INTENSITY_KEY.subkey: intensity_values}},
    )
    assert len(frame.wrong_types) == 1


def test_process_karabo_frame_one_float_attribute_not_found() -> None:
    attributes = [
        KaraboAttributeDescription(
            INTERNAL_ID1,
            DOOCS_INTENSITY_KEY,
            input_type=KaraboInputType.KARABO_TYPE_FLOAT,
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
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
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
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
            processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
            unit="mJ",
            standard_unit=True,
        )
    ]
    frame = process_karabo_frame(
        attributes,
        # Source is not given
        {},
    )
    assert len(frame.not_found) == 1


def test_frame_to_attributo_and_cache_arithmetic_mean_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            "attributo description",
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

    # Second run. It's supposed to be the arithmetic mean, so 11+1=12, 12/2=6
    second_value = 11.0
    second_frame: KaraboValueByInternalId = {INTERNAL_ID1: second_value}
    new_accumulator, new_values = frame_to_attributo_and_cache(
        second_frame, attributi, new_accumulator
    )

    assert new_values[ATTRIBUTO_ID1] == 6.0


def test_frame_to_attributo_and_cache_variance_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            "attributo description",
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

    assert new_values[ATTRIBUTO_ID1] == 50.0


def test_frame_to_attributo_and_cache_take_last_plain_attribute() -> None:
    initial_value = 1.0
    first_frame: KaraboValueByInternalId = {INTERNAL_ID1: initial_value}
    attributi = [
        AmarcordAttributoDescription(
            ATTRIBUTO_ID1,
            "attributo description",
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
            "attributo description",
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
            "attributo description",
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
            "attributo description",
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
            "attributo description",
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
            # "input-type": "List[float]",
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
            # "processor": "take-last",
            "unit": "nC",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


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
    assert attributes.processor == KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST


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
            "processor": "take-last",
        },
        KaraboValueLocator("source", "subkey"),
        0,
        {},
    )
    assert isinstance(attributes, KaraboAttributeDescription)


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
            "processor": "take-last",
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


def test_parse_attributi_not_a_dict():
    attributes = parse_amarcord_attributi(
        [],
        set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_duplicate_id():
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi={AttributoId("a")},
        attributo_description={},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_description_id_not_string():
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"description": 1},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_processor_wrong():
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"processor": "foo"},
        internal_ids=set(),
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_plain_attribute_not_found():
    attributes = parse_amarcord_attributo(
        attributo_id_raw="a",
        existing_attributi=set(),
        attributo_description={"processor": "take-last", "plain-attribute": "foO"},
        internal_ids={KaraboInternalId("foo")},
    )
    assert isinstance(attributes, KaraboConfigurationError)
    logger.warning(attributes)


def test_parse_attributo_plain_attribute():
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


def test_parse_attributo_plain_attribute_wrong_processor():
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


def test_parse_attributo_plain_attribute_both_coagulate_and_simple():
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


def test_parse_attributo_output_missing():
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


def test_parse_attributo_coagulate_list_invalid_id():
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


def test_parse_attributo_coagulate_list_no_components():
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


def test_parse_attributo_coagulate_list():
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


def test_parse_attributo_coagulate_string_invalid_id():
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


def test_parse_attributo_coagulate_string():
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
    assert len(attributes.karabo_value_source.valueSequence) == 3
    assert attributes.karabo_value_source.valueSequence[0] == PlainAttribute(
        KaraboInternalId("foo")
    )
    assert attributes.karabo_value_source.valueSequence[1] == " "
    assert attributes.karabo_value_source.valueSequence[2] == PlainAttribute(
        KaraboInternalId("bar")
    )


def test_parse_attributi():
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


def test_parse_configuration_empty():
    config = parse_configuration({})
    assert isinstance(config, KaraboConfigurationError)


def test_parse_configuration_heterogeneous_list():
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


def test_parse_configuration():
    config = parse_configuration(
        {
            CONFIG_KARABO_ATTRIBUTES_KEY: {
                "source": {
                    "subkey": {
                        "id": "internal-id",
                        "input-type": "List[float]",
                        "processor": "take-last",
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
    with (Path(__file__).parent / "karabo_online" / "config-new.yml").open("r") as f:
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
    data[configuration.special_attributes.runTrainsInRunKey.source][
        # What the fuck does pylint not understand here?!
        # pylint: disable=no-member
        configuration.special_attributes.runTrainsInRunKey.subkey
    ] = 0

    frame_output3 = karabo2.process_frame(metadata, data)
    assert frame_output3 is not None
    assert frame_output3.run_id == 53
    assert frame_output3.attributi_values[AttributoId("pulse_energy_avg")] > 500  # type: ignore
    # Variance still None because we need two values
    assert frame_output3.attributi_values[AttributoId("pulse_energy_variance")] is None
    assert frame_output3.attributi_values[AttributoId("beam_position")][0] > 1.9  # type: ignore
    assert frame_output3.attributi_values[AttributoId("beam_position")][1] < -0.7  # type: ignore
    assert frame_output3.attributi_values[ATTRIBUTO_STARTED] is not None
    assert ATTRIBUTO_STOPPED not in frame_output3.attributi_values

    # Same frame: expect a variance now
    frame_output4 = karabo2.process_frame(metadata, data)
    assert (
        frame_output4.attributi_values[AttributoId("pulse_energy_variance")] is not None  # type: ignore
    )

    # Now we change trains_in_run to a specific value again, indicating a new run has stopped
    # What the fuck does pylint not understand here?!
    # pylint: disable=no-member
    data[configuration.special_attributes.runTrainsInRunKey.source][
        # What the fuck does pylint not understand here?!
        # pylint: disable=no-member
        configuration.special_attributes.runTrainsInRunKey.subkey
    ] = 1

    frame_output5 = karabo2.process_frame(metadata, data)
    assert frame_output5.attributi_values[ATTRIBUTO_STOPPED] is not None  # type: ignore

    logger.warning(frame_output5.attributi_values)  # type: ignore


async def _get_db() -> AsyncDB:
    context = AsyncDBContext("sqlite+aiosqlite://")
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await context.create_all(CreationMode.DONT_CHECK)
    return db


async def test_karabo2_with_db() -> None:
    with (Path(__file__).parent / "karabo_online" / "config-new.yml").open("r") as f:
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

    _bridge_output = karabo2.process_frame(metadata, data)

    # We change trains_in_run to 0, indicating a new run has started
    data[configuration.special_attributes.runTrainsInRunKey.source][
        # What the fuck does pylint not understand here?!
        # pylint: disable=no-member
        configuration.special_attributes.runTrainsInRunKey.subkey
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
                processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
                unit="ml",
                standard_unit=True,
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
                    processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
                    unit="ml",
                    standard_unit=True,
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
                processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
                unit="ml",
                standard_unit=True,
            ),
            KaraboAttributeDescription(
                id=KaraboInternalId("b"),
                locator=KaraboValueLocator("source", "subkey"),
                input_type=KaraboInputType.KARABO_TYPE_FLOAT,
                processor=KaraboProcessor.KARABO_PROCESSOR_TAKE_LAST,
                unit="ml",
                standard_unit=True,
            ),
        ],
    ) == AttributoTypeList(
        sub_type=AttributoTypeDecimal(range=None, suffix="ml", standard_unit=True),
        min_length=None,
        max_length=None,
    )
