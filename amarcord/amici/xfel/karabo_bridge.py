import datetime
import logging
import re
from dataclasses import dataclass
from difflib import get_close_matches
from enum import Enum, auto
from math import fsum, isnan
from typing import (
    Dict,
    List,
    Union,
    Any,
    Iterable,
    Set,
    Optional,
    Tuple,
    cast,
)

import numpy as np
from numpy import mean, std
from pint import UnitRegistry

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import Connection
from amarcord.db.asyncdb import AsyncDB, LIVE_STREAM_IMAGE
from amarcord.db.attributi import (
    ATTRIBUTO_STARTED,
    ATTRIBUTO_STOPPED,
    datetime_from_attributo_int,
)
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoType,
    AttributoTypeDecimal,
    AttributoTypeString,
    AttributoTypeList,
    AttributoTypeInt,
)
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbattributo import DBAttributo

SPECIAL_ATTRIBUTE_JET_STREAM_IMAGE = "jetStreamImage"

logger = logging.getLogger(__name__)

# Train ID is taken from the ".tid" field in the metadata
TRAIN_ID_FROM_METADATA_KEY = "timestamp.tid"
# This is used to create the AMARCORD attributi
KARABO_ATTRIBUTO_GROUP = "karabo"
ATTRIBUTO_ID_DARK_RUN_TYPE = AttributoId("dark_type")

# These are the plain text keys for the yaml file
CONFIG_KARABO_ATTRIBUTES_KEY = "karabo-attributes"
CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY = "amarcord-attributi"
CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES = "special-karabo-attributes"
# The proposal ID is not a special attribute but given in the config so we can compare it with the incoming trains
CONFIG_KARABO_PROPOSAL = "proposal-id"
CONFIG_KARABO_SPECIAL_ATTRIBUTES_SOURCE = "source"
CONFIG_KARABO_SPECIAL_ATTRIBUTES_KEY = "key"


@dataclass(frozen=True, eq=True)
class KaraboValueLocator:
    """
    Represents a value from the Karabo bridge. The bridge gives us a dictionary with the source name as key. Below that,
    we get a dictionary of values. So we need a 2-tuple to retrieve any Karabo value, which is what this class represents
    """

    source: str
    subkey: str

    def __str__(self) -> str:
        return f"{self.source}:{self.subkey}"

    def __repr__(self) -> str:
        return f"{self.source}:{self.subkey}"


# This ID links karabo attributi (indexed by KaraboKey) to AMARCORD attributi (in a n:m fashion)
@dataclass(frozen=True, eq=True)
class KaraboInternalId:
    value: str

    def __repr__(self) -> str:
        return self.value

    def __str__(self) -> str:
        return self.value


KaraboValue = Union[str, float, int, List[float]]


class KaraboInputType(Enum):
    KARABO_TYPE_LIST_FLOAT = "List[float]"
    KARABO_TYPE_FLOAT = "float"
    KARABO_TYPE_INT = "int"
    KARABO_TYPE_STRING = "str"

    def is_scalar(self) -> bool:
        return self in (  # type: ignore
            self.KARABO_TYPE_FLOAT,
            self.KARABO_TYPE_INT,
            self.KARABO_TYPE_STRING,
        )

    def is_decimal(self):
        return self in (self.KARABO_TYPE_FLOAT, self.KARABO_TYPE_LIST_FLOAT)

    def is_numeric(self):
        return self.is_decimal() or self == self.KARABO_TYPE_INT


class KaraboProcessor(Enum):
    KARABO_PROCESSOR_IDENTITY = "identity"
    KARABO_PROCESSOR_LIST_TAKE_LAST = "list-take-last"
    KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN = "list-arithmetic-mean"
    KARABO_PROCESSOR_LIST_STANDARD_DEVIATION = "list-standard-deviation"

    def is_list(self) -> bool:
        return self in (  # type: ignore
            self.KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN,
            self.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION,
            self.KARABO_PROCESSOR_LIST_TAKE_LAST,
        )


@dataclass(frozen=True)
class KaraboWrongTypeError:
    expected: KaraboInputType
    got: str
    karabo_id: KaraboInternalId


KaraboValueByInternalId = Dict[KaraboInternalId, KaraboValue]


@dataclass(frozen=True)
class ProcessedKaraboFrame:
    """
    For a description of this, see the function that returns this structure
    """

    # We move from Karabo terminology with source and key into just our internal ID world
    karabo_values_by_internal_id: KaraboValueByInternalId
    # Important so we have non-verbose error information (i.e. we can skip errors that happened in the last frame also)
    not_found: Set[KaraboValueLocator]
    # See above
    wrong_types: Dict[KaraboValueLocator, KaraboWrongTypeError]


@dataclass(frozen=True)
class KaraboAttributeDescription:
    id: KaraboInternalId
    locator: KaraboValueLocator
    input_type: KaraboInputType
    processor: KaraboProcessor
    unit: Optional[str]
    ignore: Optional[float]
    standard_unit: bool


class AmarcordAttributoProcessor(Enum):
    AMARCORD_PROCESSOR_ARITHMETIC_MEAN = "arithmetic-mean"
    AMARCORD_PROCESSOR_VARIANCE = "variance"
    AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION = "propagated-standard-deviation"
    AMARCORD_PROCESSOR_STANDARD_DEVIATION = "standard-deviation"
    AMARCORD_PROCESSOR_TAKE_LAST = "take-last"


@dataclass(frozen=True)
class PlainAttribute:
    id: KaraboInternalId


@dataclass(frozen=True)
class CoagulateString:
    value_sequence: List[Union[str, PlainAttribute]]


@dataclass(frozen=True)
class CoagulateList:
    attributes: List[PlainAttribute]


KaraboValueSource = Union[PlainAttribute, CoagulateString, CoagulateList]


@dataclass(frozen=True, eq=True)
class AmarcordAttributoDescription:
    attributo_id: AttributoId
    description: Optional[str]
    processor: AmarcordAttributoProcessor
    karabo_value_source: KaraboValueSource


@dataclass(frozen=True)
class KaraboSpecialAttributes:
    run_number_key: KaraboValueLocator
    run_started_at_key: KaraboValueLocator
    run_first_train_key: KaraboValueLocator
    run_trains_in_run_key: KaraboValueLocator
    proposal_id_key: KaraboValueLocator
    dark_run_index_key: KaraboValueLocator
    dark_run_type_key: KaraboValueLocator
    jet_stream_image_key: Optional[KaraboValueLocator]


@dataclass(frozen=True)
class KaraboBridgeConfiguration:
    karabo_attributes: List[KaraboAttributeDescription]
    special_attributes: KaraboSpecialAttributes
    attributi: Dict[AttributoId, AmarcordAttributoDescription]
    proposal_id: int


@dataclass(frozen=True, eq=True)
class KaraboConfigurationError:
    message: str


KARABO_ATTRIBUTE_DESCRIPTION_ID = "id"
KARABO_ATTRIBUTE_DESCRIPTION_INPUT_TYPE = "input-type"
KARABO_ATTRIBUTE_DESCRIPTION_PROCESSOR = "processor"
KARABO_ATTRIBUTE_DESCRIPTION_UNIT = "unit"
KARABO_ATTRIBUTE_DESCRIPTION_IGNORE = "ignore"
KARABO_ATTRIBUTE_DESCRIPTION_SI_UNIT = "is-si-unit"


def check_type_and_processor(
    input_type: KaraboInputType, processor: KaraboProcessor
) -> Optional[str]:
    if input_type.is_scalar() and processor.is_list():
        return (
            "Got a scalar value but the processor operates on a list. If you want to keep the scalar value, "
            f'use the processor "{KaraboProcessor.KARABO_PROCESSOR_IDENTITY.value}" (or omit the processor). If you want to '
            "calculate the arithmetic mean of this value and put it into an AMARCORD attributo, use a processor on the "
            "attributo in the section {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}, not in the Karabo section."
        )
    return None


def parse_karabo_attribute(
    attribute_description: Any,
    locator: KaraboValueLocator,
    aidx: int,
    internal_ids: Dict[KaraboInternalId, Tuple[KaraboValueLocator, int]],
) -> Union[KaraboAttributeDescription, KaraboConfigurationError]:
    if not isinstance(attribute_description, dict):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}, index {aidx}: expected this to be a dictionary with a description of the attribute, but the type is {type(attribute_description)}"
        )
    internal_id_raw = attribute_description.get(KARABO_ATTRIBUTE_DESCRIPTION_ID, None)
    if internal_id_raw is None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: missing key {KARABO_ATTRIBUTE_DESCRIPTION_ID}; expected this to be a string but found nothing"
        )
    if not isinstance(internal_id_raw, str):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {KARABO_ATTRIBUTE_DESCRIPTION_ID} is not of type string but {type(internal_id_raw)}"
        )
    internal_id = KaraboInternalId(internal_id_raw)
    if internal_id in internal_ids:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {KARABO_ATTRIBUTE_DESCRIPTION_ID} is already taken by {internal_ids[internal_id]}"
        )
    internal_ids[internal_id] = (locator, aidx)
    input_type_raw = attribute_description.get(
        KARABO_ATTRIBUTE_DESCRIPTION_INPUT_TYPE, None
    )
    if input_type_raw is None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: missing key {KARABO_ATTRIBUTE_DESCRIPTION_INPUT_TYPE}; expected this to be one of "
            + (",".join(x.value for x in KaraboInputType))
            + " but found nothing"
        )
    try:
        input_type = KaraboInputType(input_type_raw)
    except ValueError:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: missing key {KARABO_ATTRIBUTE_DESCRIPTION_INPUT_TYPE}; expected this to be one of "
            + (",".join(x.value for x in KaraboInputType))
            + f' but found "{input_type_raw}"'
        )
    processor_raw = attribute_description.get(
        KARABO_ATTRIBUTE_DESCRIPTION_PROCESSOR, None
    )
    if processor_raw is None:
        if not input_type.is_scalar():
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: missing key {KARABO_ATTRIBUTE_DESCRIPTION_PROCESSOR} which is needed for anything that's not a scalar value; expected this to be one of "
                + (",".join(x.value for x in KaraboProcessor))
                + " but found nothing"
            )
        processor_raw = KaraboProcessor.KARABO_PROCESSOR_IDENTITY.value
    try:
        processor = KaraboProcessor(processor_raw)
    except ValueError:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: invalid value for {KARABO_ATTRIBUTE_DESCRIPTION_PROCESSOR}; expected this to be one of "
            + (",".join(x.value for x in KaraboProcessor))
            + f' but found "{processor_raw}"'
        )
    type_and_processor_error = check_type_and_processor(input_type, processor)
    if type_and_processor_error is not None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {type_and_processor_error}"
        )
    standard_unit = attribute_description.get(
        KARABO_ATTRIBUTE_DESCRIPTION_SI_UNIT, True
    )
    if not isinstance(standard_unit, bool):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {KARABO_ATTRIBUTE_DESCRIPTION_SI_UNIT} should be a boolean, but is {type(standard_unit)}"
        )
    unit = attribute_description.get(KARABO_ATTRIBUTE_DESCRIPTION_UNIT, None)
    if unit is not None and standard_unit:
        try:
            UnitRegistry()(unit)
        except:
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: got a unit in {KARABO_ATTRIBUTE_DESCRIPTION_UNIT}, but one we don't know: {unit}"
            )
    ignore = attribute_description.get(KARABO_ATTRIBUTE_DESCRIPTION_IGNORE, None)
    if ignore is not None:
        if not isinstance(ignore, (float, int)):
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {KARABO_ATTRIBUTE_DESCRIPTION_IGNORE} only accepts numeric values, not {ignore}"
            )
        if not input_type.is_numeric():
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: {KARABO_ATTRIBUTE_DESCRIPTION_IGNORE} specified, but type is not numeric: {input_type.value}"
            )
    if input_type.is_decimal() and standard_unit and unit is None:
        return KaraboConfigurationError(
            f'in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: you didn\'t specify a unit. Please do that or specify "{KARABO_ATTRIBUTE_DESCRIPTION_SI_UNIT}: false"'
        )
    return KaraboAttributeDescription(
        id=internal_id,
        locator=locator,
        input_type=input_type,
        processor=processor,
        unit=unit,
        standard_unit=standard_unit,
        ignore=ignore,
    )


def parse_karabo_attributes(
    a: Any,
) -> Union[KaraboConfigurationError, List[KaraboAttributeDescription]]:
    if not isinstance(a, dict):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_ATTRIBUTES_KEY}: expected this to be a dictionary with the Karabo sources as keys, but the type is {type(a)}"
        )
    internal_ids: Dict[KaraboInternalId, Tuple[KaraboValueLocator, int]] = {}
    result: List[KaraboAttributeDescription] = []
    for source, attribute_descriptions in a.items():
        if not isinstance(attribute_descriptions, dict):
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {source}: expected this to be a dictionary with the Karabo keys as keys, but the type is {type(attribute_descriptions)}"
            )
        for subkey, attribute_description_top_level in attribute_descriptions.items():
            locator = KaraboValueLocator(source, subkey)
            attribute_description_array: List[Dict[str, Any]] = []
            if isinstance(attribute_description_top_level, dict):
                attribute_description_array.append(attribute_description_top_level)
            elif isinstance(attribute_description_top_level, list):
                attribute_description_array.extend(attribute_description_top_level)
            else:
                return KaraboConfigurationError(
                    f"in {CONFIG_KARABO_ATTRIBUTES_KEY}, {locator}: expected either a dictionary with a description of the attribute, or a list of such dictionaries; got {type(attribute_description_top_level)}"
                )
            for aidx, attribute_description in enumerate(attribute_description_array):
                karabo_attribute = parse_karabo_attribute(
                    attribute_description, locator, aidx, internal_ids
                )
                if isinstance(karabo_attribute, KaraboConfigurationError):
                    return karabo_attribute
                result.append(karabo_attribute)
    return result


AMARCORD_ATTRIBUTO_DESCRIPTION_DESCRIPTION = "description"
AMARCORD_ATTRIBUTO_DESCRIPTION_PROCESSOR = "processor"
AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE = "plain-attribute"
AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE = "coagulate"

COAGULATION_INTERPOLATION_REGEX = re.compile(r"\$\{([^}]+)}")


def parse_coagulation_string(s: str) -> List[Union[str, KaraboInternalId]]:
    components: List[Union[str, KaraboInternalId]] = []
    previous_idx: Optional[int] = None
    for match in COAGULATION_INTERPOLATION_REGEX.finditer(s):
        # We have a first match, and some characters before it.
        if previous_idx is None and match.start() != 0:
            components.append(s[0 : match.start()])
            # We have another match, and some characters between the previous and this one
        elif previous_idx is not None and match.start() - previous_idx > 0:
            components.append(s[previous_idx : match.start()])
        components.append(KaraboInternalId(match.group(1)))
        previous_idx = match.end()
    # Append the tail of the string after the last match
    if previous_idx is not None and len(s) - previous_idx > 0:
        components.append(s[previous_idx:])
        # Special case: we had no matches
    elif previous_idx is None:
        components.append(s)
    return components


def parse_amarcord_attributo(
    attributo_id_raw: str,
    existing_attributi: Set[AttributoId],
    attributo_description: Dict[str, Any],
    internal_ids: Set[KaraboInternalId],
) -> Union[AmarcordAttributoDescription, KaraboConfigurationError]:
    attributo_id = AttributoId(attributo_id_raw)
    if attributo_id in existing_attributi:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}: this ID is already taken"
        )
    description = attributo_description.get(
        AMARCORD_ATTRIBUTO_DESCRIPTION_DESCRIPTION, None
    )
    if description is not None and not isinstance(description, str):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}: {AMARCORD_ATTRIBUTO_DESCRIPTION_DESCRIPTION}, not a string but {type(description)}"
        )
    processor_raw = attributo_description.get(
        AMARCORD_ATTRIBUTO_DESCRIPTION_PROCESSOR, None
    )
    if processor_raw is None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, missing key {AMARCORD_ATTRIBUTO_DESCRIPTION_PROCESSOR}; expected this to be one of "
            + (",".join(x.value for x in AmarcordAttributoProcessor))
            + " but found nothing"
        )
    try:
        processor = AmarcordAttributoProcessor(processor_raw)
    except ValueError:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, missing key {AMARCORD_ATTRIBUTO_DESCRIPTION_PROCESSOR}; expected this to be one of "
            + (",".join(x.value for x in AmarcordAttributoProcessor))
            + f" but found {processor_raw}"
        )
    plain_attribute_raw = attributo_description.get(
        AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE, None
    )
    coagulate_attribute = attributo_description.get(
        AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE
    )
    karabo_value_source: KaraboValueSource
    if plain_attribute_raw is not None and coagulate_attribute is not None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, both {AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE} and {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE} are given; choose one of them!"
        )

    if plain_attribute_raw is None and coagulate_attribute is None:
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, neither {AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE} nor {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE} are given; choose one of them!"
        )

    if plain_attribute_raw is not None:
        if not isinstance(plain_attribute_raw, str):
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE} must be a string, got {type(plain_attribute_raw)}"
            )
        plain_attribute_id = KaraboInternalId(plain_attribute_raw)
        if plain_attribute_id not in internal_ids:
            close_matches = get_close_matches(
                plain_attribute_raw, [s.value for s in internal_ids], n=1
            )
            # the pycharm type checker is stupid here and doesn't realize close_matches[0] is a str.
            # It thinks it's a Sequence[T] for some T.
            # noinspection PyTypeChecker
            return KaraboConfigurationError(
                f'in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_PLAIN_ATTRIBUTE}: "{plain_attribute_id}" not found in the list of Karabo attribute IDs'
                + (f' - maybe you meant "{close_matches[0]}"?' if close_matches else "")
            )
        karabo_value_source = PlainAttribute(plain_attribute_id)
    elif coagulate_attribute is not None:
        if isinstance(coagulate_attribute, list):
            attributes: List[PlainAttribute] = []
            for idx, attribute_id_raw in enumerate(coagulate_attribute):
                plain_attribute_id = KaraboInternalId(attribute_id_raw)
                if plain_attribute_id not in internal_ids:
                    close_matches = get_close_matches(
                        attribute_id_raw, [s.value for s in internal_ids], n=1
                    )
                    # the pycharm type checker is stupid here and doesn't realize close_matches[0] is a str.
                    # It thinks it's a Sequence[T] for some T.
                    # noinspection PyTypeChecker
                    return KaraboConfigurationError(
                        f'in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE}, index {idx}: "{plain_attribute_id}" not found in the list of Karabo attribute IDs'
                        + (
                            f', maybe you meant "{close_matches[0]}"?'
                            if close_matches
                            else ""
                        )
                    )
                attributes.append(PlainAttribute(plain_attribute_id))
            if not attributes:
                return KaraboConfigurationError(
                    f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE}: no components found"
                )
            karabo_value_source = CoagulateList(attributes)
        elif isinstance(coagulate_attribute, str):
            string_components: List[Union[str, PlainAttribute]] = []
            for part in parse_coagulation_string(coagulate_attribute):
                if isinstance(part, KaraboInternalId):
                    if part not in internal_ids:
                        close_matches = get_close_matches(
                            part.value, [s.value for s in internal_ids], n=1
                        )
                        # the pycharm type checker is stupid here and doesn't realize close_matches[0] is a str.
                        # It thinks it's a Sequence[T] for some T.
                        # noinspection PyTypeChecker
                        return KaraboConfigurationError(
                            f'in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE}: "{part}" not found in the list of Karabo attribute IDs'
                            + (
                                f', maybe you meant "{close_matches[0]}"?'
                                if close_matches
                                else ""
                            )
                        )
                    string_components.append(PlainAttribute(part))
                else:
                    string_components.append(part)
            karabo_value_source = CoagulateString(string_components)
        else:
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{attributo_id}, {AMARCORD_ATTRIBUTO_DESCRIPTION_COAGULATE}: I don't know how to handle the type {type(coagulate_attribute)}, only know lists and strings"
            )

    # noinspection PyUnboundLocalVariable
    return AmarcordAttributoDescription(
        attributo_id, description, processor, karabo_value_source
    )


def parse_amarcord_attributi(
    a: Any, internal_ids: Set[KaraboInternalId]
) -> Union[KaraboConfigurationError, List[AmarcordAttributoDescription]]:
    if not isinstance(a, dict):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}: expected this to be a dictionary with the attributi descriptions, but the type is {type(a)}"
        )
    if not internal_ids:
        return []
    existing_attributi: Set[AttributoId] = set()
    result: List[AmarcordAttributoDescription] = []
    for attributo_id_raw, attributo_description in a.items():
        if not isinstance(attributo_description, dict):
            return KaraboConfigurationError(
                f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}: attributo {attributo_id_raw}: expected this to be a dictionary with the attributi description, but the type is {type(attributo_description)}"
            )
        attributo_result = parse_amarcord_attributo(
            attributo_id_raw, existing_attributi, attributo_description, internal_ids
        )

        if isinstance(attributo_result, KaraboConfigurationError):
            return attributo_result

        result.append(attributo_result)
    return result


def parse_configuration(
    config: Any,
) -> Union[KaraboConfigurationError, KaraboBridgeConfiguration]:
    if not isinstance(config, dict):
        return KaraboConfigurationError(
            f"Expected a Python dictionary for the configuration, but the thing I got had type {type(config)}.\n\nThis is a programming mistake and needs to be fixed."
        )
    if not config.keys():
        return KaraboConfigurationError(
            f'Didn\'t find any configuration keys. Expected at least "{CONFIG_KARABO_ATTRIBUTES_KEY}".'
        )
    karabo_attributes_value = config.get(CONFIG_KARABO_ATTRIBUTES_KEY, None)
    if karabo_attributes_value is None:
        return KaraboConfigurationError(
            f"The dictionary I got for the Karabo configuration didn't contain the key {CONFIG_KARABO_ATTRIBUTES_KEY}. Maybe it was a typo? The keys I have are:\n\n- "
            + ("\n- ".join(str(s) for s in config.keys()))
        )
    karabo_attributes = parse_karabo_attributes(karabo_attributes_value)

    if isinstance(karabo_attributes, KaraboConfigurationError):
        return karabo_attributes

    attributi_value = config.get(CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY, None)
    if attributi_value is None:
        return KaraboConfigurationError(
            f"The dictionary I got for the Karabo configuration didn't contain the key {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}. Maybe it was a typo? The keys I have are:\n\n- "
            + ("\n- ".join(str(s) for s in config.keys()))
        )
    attributi = parse_amarcord_attributi(
        attributi_value, {x.id for x in karabo_attributes}
    )

    if isinstance(attributi, KaraboConfigurationError):
        return attributi

    identity_processor_check = check_for_identity_processor_and_wrong_lists(
        attributi, karabo_attributes
    )
    if identity_processor_check is not None:
        return identity_processor_check

    heterogeneous_check = check_for_heterogeneous_lists(attributi, karabo_attributes)
    if heterogeneous_check is not None:
        return heterogeneous_check

    check_amarcord_propagated_stdev_processors = (
        check_amarcord_attributi_with_processor_propagated_stdev(
            attributi, karabo_attributes
        )
    )
    if check_amarcord_propagated_stdev_processors is not None:
        return check_amarcord_propagated_stdev_processors

    cross_check_attributi_against_karabo_list_stdev_attributes = (
        check_list_stdev_karabo_attributes(attributi, karabo_attributes)
    )
    if cross_check_attributi_against_karabo_list_stdev_attributes is not None:
        return cross_check_attributi_against_karabo_list_stdev_attributes

    special_attributes = config.get(CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES, None)
    if special_attributes is None:
        return KaraboConfigurationError(
            f"The dictionary I got for the Karabo configuration didn't contain the key {CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}. Maybe it was a typo? The keys I have are:\n\n- "
            + ("\n- ".join(str(s) for s in config.keys()))
        )

    if not isinstance(special_attributes, dict):
        return KaraboConfigurationError(
            f"{CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}: not a dictionary but {type(special_attributes)}"
        )

    def search_special_attribute(
        aid: str,
    ) -> Union[KaraboConfigurationError, KaraboValueLocator]:
        special_attribute = special_attributes.get(aid, None)
        if special_attribute is None:
            return KaraboConfigurationError(
                f"{CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}, attribute {aid} not found, but it's mandatory"
            )
        if not isinstance(special_attribute, dict):
            return KaraboConfigurationError(
                f"{CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}, attribute {aid}: not specified by a dictionary but {type(special_attribute)}"
            )
        source = special_attribute.get(CONFIG_KARABO_SPECIAL_ATTRIBUTES_SOURCE, None)
        if source is None:
            return KaraboConfigurationError(
                f"{CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}, attribute {aid}: I need {CONFIG_KARABO_SPECIAL_ATTRIBUTES_SOURCE} and {CONFIG_KARABO_SPECIAL_ATTRIBUTES_KEY}, missing {CONFIG_KARABO_SPECIAL_ATTRIBUTES_SOURCE}"
            )
        key = special_attribute.get(CONFIG_KARABO_SPECIAL_ATTRIBUTES_KEY, None)
        if key is None:
            return KaraboConfigurationError(
                f"{CONFIG_KARABO_SPECIAL_KARABO_ATTRIBUTES}, attribute {aid}: I need {CONFIG_KARABO_SPECIAL_ATTRIBUTES_SOURCE} and {CONFIG_KARABO_SPECIAL_ATTRIBUTES_KEY}, missing {CONFIG_KARABO_SPECIAL_ATTRIBUTES_KEY}"
            )
        return KaraboValueLocator(source=source, subkey=key)

    run_number_key = search_special_attribute("runNumber")
    if isinstance(run_number_key, KaraboConfigurationError):
        return run_number_key
    run_started_at_key = search_special_attribute("runStartedAt")
    if isinstance(run_started_at_key, KaraboConfigurationError):
        return run_started_at_key
    run_first_train_key = search_special_attribute("runFirstTrain")
    if isinstance(run_first_train_key, KaraboConfigurationError):
        return run_first_train_key
    run_trains_in_run_key = search_special_attribute("runTrainsInRun")
    if isinstance(run_trains_in_run_key, KaraboConfigurationError):
        return run_trains_in_run_key
    proposal_id_key = search_special_attribute("proposalId")
    if isinstance(proposal_id_key, KaraboConfigurationError):
        return proposal_id_key
    dark_run_index_key = search_special_attribute("darkRunIndex")
    if isinstance(dark_run_index_key, KaraboConfigurationError):
        return dark_run_index_key
    dark_run_type_key = search_special_attribute("darkRunType")
    if isinstance(dark_run_type_key, KaraboConfigurationError):
        return dark_run_type_key
    jet_stream_image_key: Optional[KaraboValueLocator] = None
    if SPECIAL_ATTRIBUTE_JET_STREAM_IMAGE in special_attributes:
        jet_stream_image_key_result = search_special_attribute(
            SPECIAL_ATTRIBUTE_JET_STREAM_IMAGE
        )
        if isinstance(jet_stream_image_key_result, KaraboConfigurationError):
            return jet_stream_image_key_result
        jet_stream_image_key = jet_stream_image_key_result

    proposal_id = config.get(CONFIG_KARABO_PROPOSAL, None)
    if proposal_id is None:
        return KaraboConfigurationError(
            f"{CONFIG_KARABO_PROPOSAL}: missing - expected a proposal ID"
        )

    return KaraboBridgeConfiguration(
        karabo_attributes=karabo_attributes,
        special_attributes=KaraboSpecialAttributes(
            run_number_key=run_number_key,
            run_started_at_key=run_started_at_key,
            run_trains_in_run_key=run_trains_in_run_key,
            run_first_train_key=run_first_train_key,
            proposal_id_key=proposal_id_key,
            dark_run_index_key=dark_run_index_key,
            dark_run_type_key=dark_run_type_key,
            jet_stream_image_key=jet_stream_image_key,
        ),
        attributi={k.attributo_id: k for k in attributi},
        proposal_id=proposal_id,
    )


def check_for_identity_processor_and_wrong_lists(
    attributi: List[AmarcordAttributoDescription],
    karabo_attributes: List[KaraboAttributeDescription],
) -> Optional[KaraboConfigurationError]:
    karabo_attributes_per_id: Dict[KaraboInternalId, KaraboAttributeDescription] = {
        a.id: a for a in karabo_attributes
    }
    for a in attributi:
        if isinstance(a.karabo_value_source, PlainAttribute):
            # We can assume we have the value here, since we made some checks before
            karabo_attribute = karabo_attributes_per_id[a.karabo_value_source.id]
            if (
                karabo_attribute.processor == KaraboProcessor.KARABO_PROCESSOR_IDENTITY
                and not karabo_attribute.input_type.is_scalar()
                and a.processor
                != AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST
            ):
                return KaraboConfigurationError(
                    f"{CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{a.attributo_id}: refers to Karabo attribute {karabo_attribute.id}, which is a list. You currently cannot use the processor {a.processor.value} for lists. Use {AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST.value} instead."
                )

        if isinstance(a.karabo_value_source, CoagulateList):
            for kat in a.karabo_value_source.attributes:
                karabo_attribute = karabo_attributes_per_id[kat.id]
                if (
                    karabo_attribute.processor
                    == KaraboProcessor.KARABO_PROCESSOR_IDENTITY
                    and not karabo_attribute.input_type.is_scalar()
                ):
                    return KaraboConfigurationError(
                        f"{CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}:{a.attributo_id}: refers to Karabo attribute {karabo_attribute.id}, which is a list. This would create a list of lists, which are currently not supported."
                    )
    return None


def check_for_heterogeneous_lists(
    attributi: List[AmarcordAttributoDescription],
    karabo_attributes: List[KaraboAttributeDescription],
) -> Optional[KaraboConfigurationError]:
    karabo_attributes_per_id: Dict[KaraboInternalId, KaraboAttributeDescription] = {
        a.id: a for a in karabo_attributes
    }
    for a in attributi:
        if not isinstance(a.karabo_value_source, CoagulateList):
            continue
        prior_type: Optional[AttributoType] = None
        for component in a.karabo_value_source.attributes:
            this_type = determine_attributo_type_for_single_attributo(
                karabo_attributes_per_id[component.id]
            )
            if prior_type is not None and this_type != prior_type:
                return KaraboConfigurationError(
                    f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}: attributo {a.attributo_id}: the types of all dependent Karabo attributes should be the same; I got {this_type} and {prior_type}"
                )
            prior_type = this_type
    return None


def check_list_stdev_karabo_attributes(
    amarcord_attributi: List[AmarcordAttributoDescription],
    karabo_attributes: List[KaraboAttributeDescription],
) -> Optional[KaraboConfigurationError]:
    karabo_attributes_list_of_stdev: Set[KaraboInternalId] = set()

    for ka in karabo_attributes:
        if ka.processor == KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION:
            karabo_attributes_list_of_stdev.add(ka.id)

    for amarcord_attributo in amarcord_attributi:
        if (
            amarcord_attributo.processor
            != AmarcordAttributoProcessor.AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION
        ):
            relevant_karabo_ids = find_all_karabo_ids_for_amarcord_attribute(
                amarcord_attributo
            )
            for kid in relevant_karabo_ids:
                if kid in karabo_attributes_list_of_stdev:
                    return KaraboConfigurationError(
                        f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}: "
                        + f"attributo {amarcord_attributo.attributo_id}: "
                        + f"karabo attribute with id {kid} can only be processed {AmarcordAttributoProcessor.AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION.value}"
                    )
    return None


def check_amarcord_attributi_with_processor_propagated_stdev(
    amarcord_attributi: List[AmarcordAttributoDescription],
    karabo_attributes: List[KaraboAttributeDescription],
) -> Optional[KaraboConfigurationError]:
    karabo_attributes_per_id: Dict[KaraboInternalId, KaraboAttributeDescription] = {
        a.id: a for a in karabo_attributes
    }
    for amarcord_attributo in amarcord_attributi:
        if (
            amarcord_attributo.processor
            == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION
        ):
            relevant_karabo_ids = find_all_karabo_ids_for_amarcord_attribute(
                amarcord_attributo
            )
            for karabo_id in relevant_karabo_ids:
                error_configuration_propagated_stdev = check_karabo_attribute_list_of_stdev_for_prop_stdev_amarcord_processor(
                    amarcord_attributo, karabo_attributes_per_id, karabo_id
                )
                if error_configuration_propagated_stdev is not None:
                    return error_configuration_propagated_stdev

    return None


def find_all_karabo_ids_for_amarcord_attribute(
    amarcord_attributo: AmarcordAttributoDescription,
) -> Set[KaraboInternalId]:
    relevant_karabo_ids: Set[KaraboInternalId] = set()

    if isinstance(amarcord_attributo.karabo_value_source, CoagulateString):
        for component in amarcord_attributo.karabo_value_source.value_sequence:
            if not isinstance(component, PlainAttribute):
                continue
            relevant_karabo_ids.add(component.id)
    if isinstance(amarcord_attributo.karabo_value_source, CoagulateList):
        for component in amarcord_attributo.karabo_value_source.attributes:
            relevant_karabo_ids.add(component.id)
    if isinstance(amarcord_attributo.karabo_value_source, PlainAttribute):
        relevant_karabo_ids.add(amarcord_attributo.karabo_value_source.id)
    return relevant_karabo_ids


def check_karabo_attribute_list_of_stdev_for_prop_stdev_amarcord_processor(
    amarcord_attributo: AmarcordAttributoDescription,
    karabo_attributes_per_id: Dict[KaraboInternalId, KaraboAttributeDescription],
    karabo_attribute_id: KaraboInternalId,
) -> Union[KaraboConfigurationError, None]:
    karabo_attribute = karabo_attributes_per_id[karabo_attribute_id]
    if (
        karabo_attribute.processor
        != KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION
    ):
        return KaraboConfigurationError(
            f"in {CONFIG_KARABO_AMARCORD_ATTRIBUTI_KEY}: attributo {amarcord_attributo.attributo_id}: "
            + f"the type of all dependent Karabo attributes should be the {KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION.value}"
        )
    return None


def karabo_type_matches(
    input_type: KaraboInputType, value: Any
) -> Optional[KaraboValue]:
    if input_type == KaraboInputType.KARABO_TYPE_FLOAT:
        if isinstance(value, (float, int, np.floating)):
            return float(value)
        return None
    if input_type == KaraboInputType.KARABO_TYPE_INT:
        if isinstance(value, (int, np.integer)):
            return int(value)
        return None
    if input_type == KaraboInputType.KARABO_TYPE_STRING:
        if isinstance(value, str):
            return value
        return None
    assert (
        input_type == KaraboInputType.KARABO_TYPE_LIST_FLOAT
    ), f"couldn't find input type {input_type}, maybe that's a new one?"
    if isinstance(value, list):
        if not value:
            return value
        if isinstance(value[0], (int, float)):
            return value
    if isinstance(value, np.ndarray):
        # noinspection PyTypeChecker
        return value.flatten().tolist()
    return None


def run_karabo_processor(
    karabo_id: KaraboInternalId,
    processor: KaraboProcessor,
    input_type: KaraboInputType,
    ignore: Optional[float],
    value: Any,
) -> Union[KaraboWrongTypeError, Optional[KaraboValue]]:
    typed_value = karabo_type_matches(input_type, value)
    if typed_value is None:
        return KaraboWrongTypeError(input_type, str(type(value)), karabo_id)

    if processor == KaraboProcessor.KARABO_PROCESSOR_IDENTITY:
        if ignore is not None and typed_value == ignore:
            return None
        return typed_value

    if processor == KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST:
        assert isinstance(typed_value, list)
        if ignore is not None:
            typed_value = [x for x in typed_value if x != ignore]
        if typed_value:
            return typed_value[-1]
        return None

    assert isinstance(
        typed_value, list
    ), f"for the list processors, we need lists as input type, but got {typed_value}"
    if ignore is not None:
        typed_value = [x for x in typed_value if x != ignore]

    if processor == KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION:
        return std(typed_value) if len(typed_value) > 1 else 0.0

    assert (
        processor == KaraboProcessor.KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN
    ), f"unknown processor {processor}, maybe that's a new one?"
    return mean(typed_value)


def process_karabo_frame(
    attributes: Iterable[KaraboAttributeDescription], input_data: Dict[str, Any]
) -> ProcessedKaraboFrame:
    """
    Take an attribute description as well as some input data (from the bridge, probably via ZeroMQ) and produce a
    "processed Karabo frame".

    This frame is simpler than the whole input_data structure. It doesn't contain any lists anymore because
    those you have to process to scalars, and also we use our internal ID for accessing values, not source and key anymore.

    It also doesn't contain any extraneous data and some error information.
    """
    output_data: Dict[KaraboInternalId, KaraboValue] = {}
    not_found: Set[KaraboValueLocator] = set()
    wrong_types: Dict[KaraboValueLocator, KaraboWrongTypeError] = {}
    for a in attributes:
        source_values = input_data.get(a.locator.source)
        if source_values is None:
            not_found.add(a.locator)
            continue
        assert isinstance(
            source_values, dict
        ), f"{a.locator}: source isn't even a dict but {type(source_values)}, probably a typo with the source name"
        value = source_values.get(a.locator.subkey)
        if value is None:
            not_found.add(a.locator)
            continue
        processing_result = run_karabo_processor(
            a.id, a.processor, a.input_type, a.ignore, value
        )
        if isinstance(processing_result, KaraboWrongTypeError):
            wrong_types[a.locator] = processing_result
        elif processing_result is not None:
            output_data[a.id] = processing_result
    return ProcessedKaraboFrame(
        karabo_values_by_internal_id=output_data,
        not_found=not_found,
        wrong_types=wrong_types,
    )


@dataclass(frozen=True)
class TakeLastAccumulator:
    value: Union[str, int, float, List[float], None, np.ndarray]


@dataclass(frozen=True)
class MeanAccumulator:
    current_sum: float
    count: int


@dataclass(frozen=True)
class VarianceAccumulator:
    values: List[float]


@dataclass(frozen=True)
class StdDevAccumulator:
    values: List[float]


@dataclass(frozen=True)
class PropagatedStdDevAccumulator:
    current_sum: float
    count: int


@dataclass(frozen=True)
class IndexedAccumulator:
    values: List["AttributoAccumulator"]


AttributoAccumulator = Union[
    IndexedAccumulator,
    MeanAccumulator,
    PropagatedStdDevAccumulator,
    StdDevAccumulator,
    TakeLastAccumulator,
    VarianceAccumulator,
]

AttributoAccumulatorPerId = Dict[AttributoId, AttributoAccumulator]

AttributoValuePerId = Dict[AttributoId, AttributoValue]


def karabo_value_to_attributo_value(karabo_value: KaraboValue) -> AttributoValue:
    return cast(AttributoValue, karabo_value)


def process_mean_accumulator_with_plain_attribute(
    accumulator: Optional[AttributoAccumulator],
    aid: AttributoId,
    value_in_frame: Optional[KaraboValue],
) -> Tuple[AttributoAccumulator, AttributoValue]:
    assert accumulator is None or isinstance(accumulator, MeanAccumulator)
    assert value_in_frame is not None, f"attributo {aid}: not found in frame"
    if not isinstance(value_in_frame, (float, int)):
        raise Exception(
            f"attributo {aid}: cannot take the mean of value {value_in_frame}"
        )
    current_sum = (
        value_in_frame
        if accumulator is None
        else fsum([accumulator.current_sum, value_in_frame])
    )
    count = 1 if accumulator is None else accumulator.count + 1

    return MeanAccumulator(current_sum, count), (current_sum / count)


def process_variance_accumulator_with_plain_attribute(
    accumulator: Optional[AttributoAccumulator],
    aid: AttributoId,
    value_in_frame: Optional[KaraboValue],
) -> Tuple[AttributoAccumulator, AttributoValue]:
    assert accumulator is None or isinstance(accumulator, VarianceAccumulator)
    assert value_in_frame is not None, f"attributo {aid}: not found in frame"
    if not isinstance(value_in_frame, (float, int)):
        raise Exception(
            f"attributo {aid}: cannot take the mean of value {value_in_frame}"
        )

    new_array = (
        [value_in_frame]
        if accumulator is None
        else cast(VarianceAccumulator, accumulator).values + [float(value_in_frame)]
    )
    if accumulator is None:
        return VarianceAccumulator(new_array), None
    return VarianceAccumulator(new_array), np.var(new_array)


def process_propagated_stdev_accumulator_with_plain_attribute(
    accumulator: Optional[AttributoAccumulator],
    aid: AttributoId,
    value_in_frame: Optional[KaraboValue],
) -> Tuple[AttributoAccumulator, AttributoValue]:
    assert accumulator is None or isinstance(accumulator, PropagatedStdDevAccumulator)
    assert value_in_frame is not None, f"attributo {aid}: not found in frame"
    if not isinstance(value_in_frame, (float, int)):
        raise Exception(
            f"attributo {aid}: cannot take the standard deviation of value {value_in_frame}"
        )
    new_sum = (
        value_in_frame
        if accumulator is None
        else fsum([accumulator.current_sum, value_in_frame**2.0])
    )
    count = 1 if accumulator is None else accumulator.count + 1

    if accumulator is None:
        return PropagatedStdDevAccumulator(new_sum, count), None

    return PropagatedStdDevAccumulator(new_sum, count), (new_sum / count**2.0) ** 0.5


def process_stdev_from_values_accumulator_with_plain_attribute(
    accumulator: Optional[AttributoAccumulator],
    aid: AttributoId,
    value_in_frame: Optional[KaraboValue],
) -> Tuple[AttributoAccumulator, AttributoValue]:
    assert accumulator is None or isinstance(accumulator, StdDevAccumulator)
    assert value_in_frame is not None, f"attributo {aid}: not found in frame"
    if not isinstance(value_in_frame, (float, int)):
        raise Exception(
            f"attributo {aid}: cannot take the standard deviation of value {value_in_frame}"
        )
    new_array = (
        [value_in_frame]
        if accumulator is None
        else cast(StdDevAccumulator, accumulator).values + [float(value_in_frame)]
    )
    if accumulator is None:
        return StdDevAccumulator(new_array), None

    return StdDevAccumulator(new_array), std(new_array)


def process_plain_attribute(
    frame: KaraboValueByInternalId,
    aid: AttributoId,
    plain_attribute: PlainAttribute,
    accumulator: Optional[AttributoAccumulator],
    processor: AmarcordAttributoProcessor,
) -> Optional[Tuple[AttributoAccumulator, AttributoValue]]:
    value_in_frame = frame.get(plain_attribute.id, None)
    if value_in_frame is None:
        return None

    if processor == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_TAKE_LAST:
        return TakeLastAccumulator(value_in_frame), value_in_frame
    if processor == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_ARITHMETIC_MEAN:
        return process_mean_accumulator_with_plain_attribute(
            accumulator, aid, value_in_frame
        )
    if (
        processor
        == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_PROPAGATED_STANDARD_DEVIATION
    ):
        return process_propagated_stdev_accumulator_with_plain_attribute(
            accumulator, aid, value_in_frame
        )
    if processor == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_STANDARD_DEVIATION:
        return process_stdev_from_values_accumulator_with_plain_attribute(
            accumulator, aid, value_in_frame
        )

    assert (
        processor == AmarcordAttributoProcessor.AMARCORD_PROCESSOR_VARIANCE
    ), f"I don't know the processor {processor}, maybe it's new?"

    return process_variance_accumulator_with_plain_attribute(
        accumulator, aid, value_in_frame
    )


def process_coagulate_list(
    frame: KaraboValueByInternalId,
    accumulators: AttributoAccumulatorPerId,
    aid: AttributoId,
    coagulate_list: CoagulateList,
    processor: AmarcordAttributoProcessor,
) -> Optional[Tuple[IndexedAccumulator, AttributoValue]]:
    accumulator = accumulators.get(aid, None)
    assert accumulator is None or isinstance(
        accumulator, IndexedAccumulator
    ), f"attributo {aid}: accumulator is {type(accumulator)}, not {IndexedAccumulator}"

    assert accumulator is None or len(accumulator.values) == len(
        coagulate_list.attributes
    )
    new_values: List[AttributoValue] = []
    new_accumulators: List[AttributoAccumulator] = []
    for idx, plain_karabo_attribute in enumerate(coagulate_list.attributes):
        new_accumulator_and_value = process_plain_attribute(
            frame,
            aid,
            plain_karabo_attribute,
            accumulator.values[idx] if accumulator is not None else None,
            processor,
        )
        if new_accumulator_and_value is None:
            return None
        new_accumulator, value = new_accumulator_and_value
        new_values.append(value)
        new_accumulators.append(new_accumulator)
    return IndexedAccumulator(new_accumulators), new_values  # type: ignore


def attributo_value_to_string(value: AttributoValue) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def process_coagulate_string(
    frame: KaraboValueByInternalId,
    accumulators: AttributoAccumulatorPerId,
    aid: AttributoId,
    coagulate_string: CoagulateString,
    processor: AmarcordAttributoProcessor,
) -> Optional[Tuple[IndexedAccumulator, str]]:
    accumulator = accumulators.get(aid, None)
    assert accumulator is None or isinstance(
        accumulator, IndexedAccumulator
    ), f"attributo {aid}: accumulator is {type(accumulator)}, not {IndexedAccumulator}"

    new_value = ""
    new_accumulators: List[AttributoAccumulator] = []
    accumulator_idx = 0
    for attribute_or_str in coagulate_string.value_sequence:
        if isinstance(attribute_or_str, str):
            new_value += attribute_or_str
            continue

        new_accumulator_and_value = process_plain_attribute(
            frame,
            aid,
            attribute_or_str,
            accumulator.values[accumulator_idx] if accumulator is not None else None,
            processor,
        )
        if new_accumulator_and_value is None:
            return None
        new_accumulator, value = new_accumulator_and_value
        accumulator_idx += 1
        new_value += attributo_value_to_string(value)
        new_accumulators.append(new_accumulator)
    return IndexedAccumulator(new_accumulators), new_value


def frame_to_attributo_and_cache(
    frame: KaraboValueByInternalId,
    attributi: Iterable[AmarcordAttributoDescription],
    old_accumulator: AttributoAccumulatorPerId,
) -> Tuple[AttributoAccumulatorPerId, AttributoValuePerId]:
    accumulators: AttributoAccumulatorPerId = old_accumulator.copy()
    values: AttributoValuePerId = {}
    for a in attributi:
        if isinstance(a.karabo_value_source, PlainAttribute):
            accumulator_and_value = process_plain_attribute(
                frame,
                a.attributo_id,
                a.karabo_value_source,
                accumulators.get(a.attributo_id, None),
                a.processor,
            )
            if accumulator_and_value is None:
                continue
            accumulator, value = accumulator_and_value
            accumulators[a.attributo_id] = accumulator
            values[a.attributo_id] = value
        elif isinstance(a.karabo_value_source, CoagulateList):
            caccumulator_and_cvalue = process_coagulate_list(
                frame, accumulators, a.attributo_id, a.karabo_value_source, a.processor
            )
            if caccumulator_and_cvalue is None:
                continue
            caccumulator, cvalue = caccumulator_and_cvalue
            accumulators[a.attributo_id] = caccumulator
            values[a.attributo_id] = cvalue
        else:
            assert isinstance(
                a.karabo_value_source, CoagulateString
            ), f"Karabo value source is {type(a.karabo_value_source)}; I only know PlainAttribute, CoagulateList and CoagulateString"
            saccumulator_and_svalue = process_coagulate_string(
                frame, accumulators, a.attributo_id, a.karabo_value_source, a.processor
            )
            if saccumulator_and_svalue is None:
                continue
            saccumulator, svalue = saccumulator_and_svalue
            accumulators[a.attributo_id] = saccumulator
            values[a.attributo_id] = svalue

    return accumulators, values


def determine_attributo_type_for_single_attributo(
    karabo_attribute: KaraboAttributeDescription,
) -> AttributoType:
    if karabo_attribute.input_type == KaraboInputType.KARABO_TYPE_FLOAT:
        return AttributoTypeDecimal(
            suffix=karabo_attribute.unit, standard_unit=karabo_attribute.standard_unit
        )
    if karabo_attribute.input_type == KaraboInputType.KARABO_TYPE_INT:
        return AttributoTypeInt()
    if karabo_attribute.input_type == KaraboInputType.KARABO_TYPE_STRING:
        return AttributoTypeString()

    assert karabo_attribute.input_type == KaraboInputType.KARABO_TYPE_LIST_FLOAT
    assert karabo_attribute.processor in (
        KaraboProcessor.KARABO_PROCESSOR_LIST_TAKE_LAST,
        KaraboProcessor.KARABO_PROCESSOR_LIST_ARITHMETIC_MEAN,
        KaraboProcessor.KARABO_PROCESSOR_LIST_STANDARD_DEVIATION,
    )
    return AttributoTypeDecimal(
        suffix=karabo_attribute.unit, standard_unit=karabo_attribute.standard_unit
    )


def determine_attributo_type(
    a: AmarcordAttributoDescription, karabo_attributes: List[KaraboAttributeDescription]
) -> AttributoType:
    if isinstance(a.karabo_value_source, CoagulateString):
        return AttributoTypeString()
    if isinstance(a.karabo_value_source, PlainAttribute):
        # Since we've type-checked this thing, we can be sure the list here is nonempty
        karabo_attribute = [
            x for x in karabo_attributes if x.id == a.karabo_value_source.id
        ][0]
        return determine_attributo_type_for_single_attributo(karabo_attribute)
    assert isinstance(a.karabo_value_source, CoagulateList)
    first_element = a.karabo_value_source.attributes[0]
    # Since we've type-checked this thing, we can be sure the list here is nonempty
    karabo_attribute = [x for x in karabo_attributes if x.id == first_element.id][0]
    return AttributoTypeList(
        sub_type=determine_attributo_type_for_single_attributo(karabo_attribute),
        min_length=None,
        max_length=None,
    )


@dataclass(frozen=True, eq=True)
class DarkRunInformation:
    index: int
    type: str


@dataclass(frozen=True)
class BridgeOutput:
    attributi_values: AttributoValuePerId
    run_id: int
    run_started_at: Optional[datetime.datetime]
    run_stopped_at: Optional[datetime.datetime]
    proposal_id: int
    dark_run: Optional[DarkRunInformation]
    image: Optional[np.ndarray]


def locate_in_frame(
    frame: Dict[str, Any], key: KaraboValueLocator
) -> Optional[KaraboValue]:
    sourced = frame.get(key.source, None)
    if sourced is None:
        return None
    return sourced.get(key.subkey, None)


class RunStatus(Enum):
    UNKNOWN = auto()
    STOPPED = auto()
    RUNNING = auto()


async def ingest_bridge_output(
    db: AsyncDB, conn: Connection, result: BridgeOutput
) -> None:
    attributi = await db.retrieve_attributi(conn, associated_table=None)
    latest_run = await db.retrieve_latest_run(conn, attributi)
    auto_pilot = (await db.retrieve_configuration(conn)).auto_pilot
    if latest_run is not None:
        if latest_run.id < result.run_id - 1:
            # For some reason we lost runs, we create dummy runs for each lost run_id
            logger.warning(
                f"Current run {result.run_id}, latest known run {latest_run.id}, filling the gaps with dummy runs"
            )
            for missing_id in range(latest_run.id + 1, result.run_id):
                await create_dummy_run_with_attributi(
                    attributi, conn, db, missing_id, auto_pilot
                )
            await create_run_with_attributi(attributi, conn, db, result, auto_pilot)
        elif latest_run.id == result.run_id - 1:
            await create_run_with_attributi(attributi, conn, db, result, auto_pilot)
        elif latest_run.id == result.run_id:
            latest_run.attributi.extend(result.attributi_values)
            await db.update_run_attributi(conn, result.run_id, latest_run.attributi)
        else:
            logger.error(
                f"The id of the current run {result.run_id} is smaller than the last known run {latest_run.id}, which does not make sense."
            )
    else:
        await create_run_with_attributi(attributi, conn, db, result, auto_pilot)
    if result.dark_run is not None:
        dark_run = await db.retrieve_run(conn, result.dark_run.index, attributi)
        if dark_run is None:
            logger.warning(f"dark run {result.dark_run.index} not found")
        else:
            logger.info(
                f"run {result.dark_run.index} is dark of type {result.dark_run.type}"
            )
            dark_run.attributi.append_single(
                ATTRIBUTO_ID_DARK_RUN_TYPE, result.dark_run.type
            )
            await db.update_run_attributi(
                conn, result.dark_run.index, dark_run.attributi
            )
    if result.image is not None:
        existing_file = await db.retrieve_file_id_by_name(conn, LIVE_STREAM_IMAGE)
        if existing_file is None:
            await db.create_image_from_nparray(
                conn,
                LIVE_STREAM_IMAGE,
                description="",
                contents=result.image,
                format_="png",
            )
        else:
            await db.update_image_from_nparray(
                conn,
                existing_file,
                file_name=LIVE_STREAM_IMAGE,
                description="",
                contents=result.image,
                format_="png",
            )


async def create_dummy_run_with_attributi(
    attributi: List[DBAttributo],
    conn: Connection,
    db: AsyncDB,
    run_id: int,
    auto_pilot: bool,
):
    logger.warning(f"creating dummy run with id {run_id}")
    await db.create_run(
        conn,
        run_id,
        attributi=attributi,
        attributi_map=AttributiMap.from_types_and_raw(
            attributi, await db.retrieve_sample_ids(conn), {}
        ),
        keep_manual_attributes_from_previous_run=auto_pilot,
    )


async def create_run_with_attributi(
    attributi: List[DBAttributo],
    conn: Connection,
    db: AsyncDB,
    result: BridgeOutput,
    auto_pilot: bool,
):
    await db.create_run(
        conn,
        result.run_id,
        attributi=attributi,
        attributi_map=AttributiMap.from_types_and_raw(
            attributi,
            await db.retrieve_sample_ids(conn),
            result.attributi_values,
        ),
        keep_manual_attributes_from_previous_run=auto_pilot,
    )


class Karabo2:
    def __init__(
        self,
        parsed_config: KaraboBridgeConfiguration,
        first_train_is_start_of_run=False,
    ) -> None:
        self._first_train_is_start_of_run = first_train_is_start_of_run
        self._accumulators: AttributoAccumulatorPerId = {}
        self._previously_not_found: Set[KaraboValueLocator] = set()
        self._previously_wrong_type: Dict[KaraboValueLocator, KaraboWrongTypeError] = {}
        self._special_attributes = parsed_config.special_attributes
        self._attributi = parsed_config.attributi
        self._karabo_attributes = parsed_config.karabo_attributes
        self._last_train_id: Optional[int] = None
        self._proposal_id = parsed_config.proposal_id
        self._last_proposal_id: Optional[int] = None
        self._previous_trains_in_run: Optional[int] = None
        self._run_status = (
            RunStatus.STOPPED
            if self._first_train_is_start_of_run
            else RunStatus.UNKNOWN
        )
        self._previous_run_status = RunStatus.UNKNOWN
        self._no_proposal_last_train = False
        self._run_started_at: Optional[datetime.datetime] = None
        self._run_stopped_at: Optional[datetime.datetime] = None

    async def create_missing_attributi(self, db: AsyncDB, conn: Connection) -> None:
        db_attributi = {
            a.name: a for a in await db.retrieve_attributi(conn, associated_table=None)
        }

        if ATTRIBUTO_ID_DARK_RUN_TYPE not in db_attributi:
            await db.create_attributo(
                conn,
                ATTRIBUTO_ID_DARK_RUN_TYPE,
                "",
                KARABO_ATTRIBUTO_GROUP,
                AssociatedTable.RUN,
                AttributoTypeString(),
            )

        for aid, a in self._attributi.items():
            existing_db_attributo = db_attributi.get(aid, None)
            a_type = determine_attributo_type(a, self._karabo_attributes)
            if existing_db_attributo is None:
                logger.info(f"karabo: creating attributo {aid}")

                await db.create_attributo(
                    conn,
                    aid,
                    a.description if a.description is not None else "",
                    KARABO_ATTRIBUTO_GROUP,
                    AssociatedTable.RUN,
                    a_type,
                )
            else:
                if a_type != existing_db_attributo.attributo_type:
                    raise Exception(
                        f"attributo {aid}: type differs! in DB: {existing_db_attributo.attributo_type}, in config: {a_type}"
                    )

    def _extract_train_id(self, metadata: Dict[str, Any]) -> Optional[int]:
        run_number_source = metadata.get(
            self._special_attributes.run_number_key.source, None
        )
        if run_number_source is None:
            logger.error(
                f"tried to retrieve source {self._special_attributes.run_number_key.source} from the metadata dictionary, but did not find it. You probably have to change something in the Karabo (bridge) configuration to make this work."
            )
            return None
        train_id = run_number_source.get(TRAIN_ID_FROM_METADATA_KEY, None)
        if train_id is None:
            logger.error(
                f"tried to retrieve the train id from source {self._special_attributes.run_number_key.source}:{TRAIN_ID_FROM_METADATA_KEY} from the metadata dictionary, but did not find it.  You probably have to change something in the Karabo (bridge) configuration to make this work."
            )
        return train_id

    def process_frame(
        self, metadata: Dict[str, Any], frame: Dict[str, Any]
    ) -> Optional[BridgeOutput]:
        train_id = self._extract_train_id(metadata)

        if train_id is None:
            logger.info("no train ID found")
            return None

        self._compare_train_ids(train_id)

        if not self._is_valid_proposal(train_id, frame):
            return None

        run_id = locate_in_frame(frame, self._special_attributes.run_number_key)
        if run_id is None:
            logger.error(f"train {train_id}: no run ID, skipping frame")
            return None
        assert isinstance(
            run_id, int
        ), f"train {train_id}: run ID is not an int but {type(run_id)}"

        trains_in_run = locate_in_frame(
            frame, self._special_attributes.run_trains_in_run_key
        )
        if trains_in_run is None:
            logger.error(f"train {train_id}: no trains in run, skipping frame")
            return None
        assert isinstance(
            trains_in_run, int
        ), f"train {train_id}: trains in run is not an int but {trains_in_run}"

        run_in_progress = (
            True if self._first_train_is_start_of_run else trains_in_run == 0
        )
        if run_in_progress and self._run_status == RunStatus.UNKNOWN:
            if self._previous_run_status is None:
                logger.info(
                    "started in the middle of a run, waiting for it to complete"
                )
                self._previous_run_status = RunStatus.UNKNOWN
            return None
        if not run_in_progress and self._run_status == RunStatus.UNKNOWN:
            logger.info("started with a finished run, waiting for next run")
            self._run_status = RunStatus.STOPPED
            return None
        if not run_in_progress and self._run_status == RunStatus.STOPPED:
            return None

        if run_in_progress and self._run_status == RunStatus.STOPPED:
            logger.info(
                f"train {train_id}: a new run started, initializing accumulators"
            )
            self._accumulators = {}
            # for whatever reason pylint will complain on the next line,
            # the rule R0204 is otherwise correctly applied throughout the file
            # pylint: disable=R0204
            self._run_status = RunStatus.RUNNING
            self._run_started_at = datetime.datetime.utcnow()
            self._run_stopped_at = None

        if not run_in_progress and self._run_status == RunStatus.RUNNING:
            logger.info(f"run {run_id} stopped")
            self._run_status = RunStatus.STOPPED
            self._run_stopped_at = datetime.datetime.utcnow()

        processed_frame = process_karabo_frame(self._karabo_attributes, frame)

        self._process_not_found_and_type_errors(train_id, processed_frame)

        new_accumulators, attributi_values = frame_to_attributo_and_cache(
            processed_frame.karabo_values_by_internal_id,
            self._attributi.values(),
            self._accumulators,
        )
        # Karabo could send NaN to us
        # pylint: disable=use-dict-comprehension
        for k, v in attributi_values.items():
            if isinstance(v, (float, np.floating)) and isnan(v):
                attributi_values[k] = None
        attributi_values[ATTRIBUTO_STARTED] = self._run_started_at
        if self._run_stopped_at is not None:
            attributi_values[ATTRIBUTO_STOPPED] = self._run_stopped_at
        self._accumulators = new_accumulators
        dark_run_index = locate_in_frame(
            frame, self._special_attributes.dark_run_index_key
        )
        dark_run_type = locate_in_frame(
            frame, self._special_attributes.dark_run_type_key
        )
        dark_run_information: Optional[DarkRunInformation]
        if dark_run_index is not None and dark_run_type is not None:
            if not isinstance(dark_run_index, int):
                logger.warning(
                    f"dark run index at {self._special_attributes.dark_run_index_key} is not int but {type(dark_run_index)}"
                )
                dark_run_information = None
            elif not isinstance(dark_run_type, str):
                logger.warning(
                    f"dark run type at {self._special_attributes.dark_run_index_key} is not a string but {type(dark_run_index)}"
                )
                dark_run_information = None
            else:
                dark_run_information = DarkRunInformation(dark_run_index, dark_run_type)
        else:
            dark_run_information = None
        image: Optional[np.ndarray]
        if self._special_attributes.jet_stream_image_key is not None:
            image_in_frame = locate_in_frame(
                frame, self._special_attributes.jet_stream_image_key
            )
            if not isinstance(image_in_frame, np.ndarray):
                logger.warning(
                    f"image at {self._special_attributes.jet_stream_image_key} isn't an nparray but {type(image_in_frame)}"
                )
                image = None
            else:
                image = image_in_frame
        else:
            image = None
        return BridgeOutput(
            attributi_values=attributi_values,
            run_id=run_id,
            proposal_id=self._proposal_id,
            run_started_at=self._run_started_at,
            run_stopped_at=self._run_stopped_at,
            dark_run=dark_run_information,
            image=image,
        )

    def _compare_train_ids(self, train_id: int) -> None:
        if self._last_train_id is not None and self._last_train_id != train_id - 1:
            logger.warning(
                f"train {train_id}: missed a train (last train {self._last_train_id})"
            )
        self._last_train_id = train_id

    def _is_valid_proposal(self, train_id: int, frame: Dict[str, Any]) -> bool:
        proposal = locate_in_frame(frame, self._special_attributes.proposal_id_key)
        if proposal is None:
            if not self._no_proposal_last_train:
                logger.warning(f"train {train_id}: no proposal, assuming it's valid")
                self._no_proposal_last_train = True
            return True
        self._no_proposal_last_train = False
        assert isinstance(
            proposal, int
        ), f"train {train_id}: proposal is not an int but {proposal}"
        if proposal != self._proposal_id:
            if self._last_proposal_id is None or self._last_proposal_id != proposal:
                logger.error(
                    f"train {train_id}: wrong proposal {proposal}, expected {self._proposal_id}; skipping frames until proposal is right"
                )
                self._last_proposal_id = proposal
            return False
        self._last_proposal_id = proposal
        return True

    def _process_not_found_and_type_errors(
        self, train_id: int, processed_frame: ProcessedKaraboFrame
    ) -> None:
        if (
            processed_frame.not_found != self._previously_not_found
            and processed_frame.not_found
        ):
            logger.warning(
                f"train {train_id}: the following attributes were not found: \n- "
                + "\n- ".join(str(s) for s in processed_frame.not_found)
            )
            self._previously_not_found = processed_frame.not_found

        if (
            processed_frame.wrong_types != self._previously_wrong_type
            and processed_frame.wrong_types
        ):
            logger.warning(
                f"train {train_id}: the following attributes had type errors: \n- "
                + "\n- ".join(
                    f"{locator} (attribute {type_error.karabo_id}): expected {type_error.expected.value}, got {type_error.got}"
                    for locator, type_error in processed_frame.wrong_types.items()
                )
            )
            self._previously_wrong_type = processed_frame.wrong_types


def accumulator_locators_for_config(
    c: KaraboBridgeConfiguration,
) -> Dict[str, Set[str]]:
    result: Dict[str, Set[str]] = {}
    for karabo_attribute in c.karabo_attributes:
        source_data = result.get(karabo_attribute.locator.source, None)
        if source_data is None:
            source_data = set()
        source_data.add(karabo_attribute.locator.subkey)
        result[karabo_attribute.locator.source] = source_data
    return result


async def persist_euxfel_run_result(
    conn: Connection,
    db: AsyncDB,
    result: BridgeOutput,
    run_id: int,
    train_timestamps: List[float],
) -> None:
    min_timestamp_unix = int(np.min(train_timestamps)) // 1000000
    max_timestamp_unix = int(np.max(train_timestamps)) // 1000000
    min_timestamp = datetime_from_attributo_int(min_timestamp_unix)
    max_timestamp = datetime_from_attributo_int(max_timestamp_unix)
    new_values = result.attributi_values.copy()
    new_values[ATTRIBUTO_STARTED] = min_timestamp
    new_values[ATTRIBUTO_STOPPED] = max_timestamp
    logger.info(f"run {run_id}: {new_values}")
    attributi = await db.retrieve_attributi(conn, associated_table=None)
    run = await db.retrieve_run(conn, run_id, attributi)
    attributi_map = AttributiMap.from_types_and_raw(
        attributi,
        sample_ids=await db.retrieve_sample_ids(conn),
        raw_attributi=new_values,
    )
    if run is None:
        await db.create_run(
            conn,
            run_id,
            attributi,
            attributi_map,
            keep_manual_attributes_from_previous_run=False,
        )
    else:
        run.attributi.extend_with_attributi_map(attributi_map)
        await db.update_run_attributi(conn, run_id, run.attributi)


def process_trains(
    config: KaraboBridgeConfiguration,
    karabo2: Karabo2,
    run_id: int,
    train_ids: List[int],
    trains: Iterable[Tuple[int, Dict[str, Any]]],
) -> Optional[BridgeOutput]:
    def optionally_set_dict(
        dictionary: Dict[str, Any], locator: KaraboValueLocator, value: Any
    ) -> None:
        if locator.source not in dictionary:
            dictionary[locator.source] = {}
        dictionary[locator.source][locator.subkey] = value

    result: Optional[BridgeOutput] = None
    for tid, data in trains:
        extended_data = data.copy()
        optionally_set_dict(
            extended_data, config.special_attributes.proposal_id_key, config.proposal_id
        )
        optionally_set_dict(
            extended_data, config.special_attributes.run_number_key, run_id
        )
        optionally_set_dict(
            extended_data,
            config.special_attributes.run_trains_in_run_key,
            len(train_ids),
        )
        result = karabo2.process_frame(
            metadata={
                config.special_attributes.run_number_key.source: {
                    TRAIN_ID_FROM_METADATA_KEY: tid
                }
            },
            frame=extended_data,
        )
    return result
