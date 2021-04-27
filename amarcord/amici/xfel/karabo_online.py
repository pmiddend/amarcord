import logging
import os
import re
from dataclasses import dataclass
from enum import Enum
from enum import auto
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import Generic
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar
from typing import TypedDict
from typing import Union
from typing import cast

import numpy as np
import yaml

from amarcord.amici.xfel.karabo_attributi import KaraboAttributi
from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_cache import KaraboCacheContent
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole
from amarcord.amici.xfel.karabo_statistics import KaraboStatistics
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_value import KaraboValue

logger = logging.getLogger(__name__)

T = TypeVar("T")


# TO BE FIXED: to be moved to another module
def load_configuration(descriptor: str) -> Dict[str, Any]:
    """Load the configuration file

    Args:
        descriptor (str): The YAML file

    Raises:
        FileNotFoundError: Self explaining

    Returns:
            Dict[str, Any]: The configuration
    """

    if os.path.exists(descriptor):
        with open(descriptor) as fh:
            configuration = yaml.load(fh, Loader=yaml.Loader)

    else:
        raise FileNotFoundError("{} not found...".format(descriptor))

    return configuration


def parse_attributo(
    identifier: str,
    global_source: Optional[str],
    global_action: Optional[str],
    action_axis: Optional[int],
    ai_content: Dict[str, Any],
) -> KaraboAttributo:
    type_ = ai_content.get("type", "decimal")

    list_re = re.fullmatch(
        r"image|int|decimal|str|datetime|list\[(int|decimal|str)]", type_
    )
    if not list_re:
        raise ValueError(
            f"Attributo type must be str, int, decimal, datetime or a list of int or decimal, is {type}"
        )

    action = ai_content.get("action", global_action)
    if action is not None and action not in [x.value for x in KaraboAttributoAction]:
        raise ValueError(
            "Action must be either '{}', is {}".format(
                "' or '".join(s.value for s in KaraboAttributoAction), action
            )
        )
    if action is not None:
        action = KaraboAttributoAction(action)
    else:
        action = KaraboAttributoAction.STORE_LAST

    source = ai_content.get("source", global_source)

    if source is None:
        raise ValueError(
            f"Attributo {identifier} has no source and no global source given!"
        )

    role = ai_content.get("role", None)
    if role is not None and role not in [x.value for x in KaraboSpecialRole]:
        raise ValueError(
            "Role must be either '{}', is {}".format(
                "' or '".join(s.value for s in KaraboSpecialRole), role
            )
        )
    if role is not None:
        role = KaraboSpecialRole(role)

    return KaraboAttributo(
        identifier=identifier,
        source=source,
        key=ai_content["key"],
        description=ai_content.get("description", None),
        type_=type_,
        store=ai_content.get("store", True),
        action=action,
        action_axis=ai_content.get("action_axis", action_axis),
        unit=ai_content.get("unit", None),
        filling_value=ai_content.get("filling_value", None),
        value=None,
        role=role,
    )


def parse_configuration(
    configuration: Dict[str, Any]
) -> Tuple[KaraboAttributi, KaraboExpectedAttributi]:
    """Parse the configuration file

    Args:
        configuration (Dict[str, Any]): The configuration

    Raises:
        TypeError: If the attributo syntax is wrong

    Returns:
        Dict[str, List[Dict[str, Any]]], Dict[str, List[str]]: A dictionary of attributi and one with expected Karabo
        keywords
    """
    entry: KaraboAttributi = {}
    karabo_expected_entry: KaraboExpectedAttributi = {}

    for (
        gi,
        gi_content,
    ) in configuration.items():
        source, action, action_axis = None, None, None

        for (
            ai,
            ai_content,
        ) in gi_content.items():

            # source and action can be set globally, for the entire group
            if ai == "source":
                source = ai_content
            elif ai == "action":
                action = ai_content
            elif ai == "action_axis":
                action_axis = ai_content

            if isinstance(ai_content, dict):
                if gi not in entry.keys():
                    entry[gi] = {}

                if ai in entry[gi]:
                    logger.warning(
                        "duplicate attributo for source %s and key %s", gi, ai
                    )
                    continue
                entry[gi][ai] = parse_attributo(
                    ai, source, action, action_axis, ai_content
                )

    # build the corresponding Karabo bridge expected entry
    for group_name, group in entry.items():
        for attributo in group.values():
            if attributo.source not in karabo_expected_entry:
                karabo_expected_entry[attributo.source] = {
                    attributo.key: {"attributo": attributo, "group": group_name}
                }
            else:
                karabo_expected_entry[attributo.source].update(
                    {attributo.key: {"attributo": attributo, "group": group_name}}
                )

    return entry, karabo_expected_entry


# TO BE FIXED: to be moved to another module
# the function currently takes `attributi` and `_attributi`, which does not make sense.
# `attributi` and `_attributi` are basically a different view of the same thing, and therefore we should probably
# return `attributi` and
# 1) have a function to fill _attributi
# 2) have a function that converts views on demand (ie. when returning to the daemon)
def compute_statistics(
    cache: KaraboCacheContent,
    expected_attributi: KaraboExpectedAttributi,
    attributi: KaraboAttributi,
    current_run: int,
) -> None:
    def remove_filling_values(
        data: Union[KaraboValue, List[KaraboValue]], filling_value: KaraboValue
    ) -> Tuple[List[KaraboValue], int]:
        if not isinstance(data, list):
            return [data], 0
        container: List[KaraboValue] = []
        removed_ = 0

        for entry in data:
            if not hasattr(entry, "__len__"):
                # remove filling value, which must be a scalar
                if entry == filling_value:
                    removed_ += 1
                    continue

            container.append(entry)

        return container, removed_

    for source, source_content in cache.items():
        for key in source_content:

            attributo = expected_attributi[source][key]["attributo"]

            # remove filling values
            cached_value = cache[source][key]
            cached_data, removed = remove_filling_values(
                cached_value,
                attributo.filling_value,
            )

            if removed:
                logger.warning(
                    "{}//{}: removed {} entries".format(source, key, removed)
                )

            # compute statistics
            if attributo.action in (
                KaraboAttributoAction.COMPUTE_STANDARD_DEVIATION,
                KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
            ):
                reduced_value = KaraboStatistics.call(
                    attributo.action,
                    cached_data,
                    axis=attributo.action_axis,
                    source=source,
                    key=key,
                )

                _group = expected_attributi[source][key]["group"]
                attributo.value = reduced_value
                attributi[_group][attributo.identifier].value = reduced_value

                logger.debug(
                    "%s on %s//%s: reduced value: %s",
                    attributo.action,
                    source,
                    key,
                    reduced_value,
                )

            # check if values are constant
            elif attributo.action == KaraboAttributoAction.CHECK_IF_CONSTANT:
                # noinspection PyArgumentList
                unique_value, unique_index = np.unique(  # type: ignore
                    cached_data,
                    axis=attributo.action_axis,
                    return_index=True,
                )

                if unique_index.size > 1:
                    logger.warning(
                        "%s//%s run %s: not constant over run; values are: %s",
                        source,
                        key,
                        current_run,
                        unique_value,
                    )

                # the if thing handles empty lists
                reduced_value = (
                    unique_value[0] if unique_value.size > 0 else unique_value
                )

                _group = expected_attributi[source][key]["group"]
                attributo.value = reduced_value

                attributi[_group][attributo.identifier].value = reduced_value

                logger.debug(
                    "%s on %s//%s: reduced value: %s",
                    attributo.action,
                    source,
                    key,
                    reduced_value,
                )
            elif attributo.action == KaraboAttributoAction.STORE_LAST:
                # Special case here: if we want to store the last value, but the value wasn't there in the
                # last train, we keep the last recorded value (which might be None as well, of course, but if not,
                # that's the better choice)
                if cached_value is not None:
                    attributo.value = cached_value


def compare_metadata_trains(metadata: Dict[str, Any]) -> int:
    """Checks if Karabo devices are synchronized

    Args:
        metadata (Dict[str, Any]): Metadata from the Karabo bridge

    Raises:
        ValueError: One or more devices are desynchronized

    Returns:
        int: Current trainId
    """
    trainId: Optional[int] = None

    for v in metadata.values():
        tid = v.get("timestamp.tid")
        if tid is None:
            raise ValueError('source without key "timestamp.tid" found')
        if trainId is None:
            trainId = tid
        elif trainId != tid:
            raise ValueError(f"Karabo devices desynchronized (tid {trainId} vs. {tid}")

    if trainId is None:
        raise ValueError("no devices found in Karabo")

    return trainId


@dataclass(frozen=True)
class KaraboAttributoWithValue(Generic[T]):
    attributo: KaraboAttributo
    value: T


class RunStatus(Enum):
    RUNNING = auto()
    CLOSED = auto()


class TrainContentDict(TypedDict):
    number: KaraboAttributoWithValue[int]
    index: KaraboAttributoWithValue[int]
    proposal_id: KaraboAttributoWithValue[int]
    train_index_initial: KaraboAttributoWithValue[int]
    timestamp_UTC_initial: KaraboAttributoWithValue[str]
    trains_in_run: KaraboAttributoWithValue[int]
    status: RunStatus


def attributo2karabo(
    attributi: KaraboAttributi,
    data: KaraboData,
    group: str,
    attributo: str,
    type_: Type[T],
) -> KaraboAttributoWithValue[T]:
    """
    From a parsed config and some data from the bridge, resolve a group and an attributo ID to a value.

    It also checks the type of the item.
    """
    try:
        ai = attributi[group][attributo]
    except KeyError:
        logger.warning(
            "Missing entry %s in attributo definition",
            attributo,
        )
        raise ValueError()

    try:
        value = data[ai.source][ai.key]
        # We allow ints as float type here
        if not isinstance(value, type_) and (
            type_ != float or not isinstance(value, int)
        ):
            logger.warning(
                "Entry %s for source %s, key '%s' in the stream doesn't have type %s but %s: %s",
                attributo,
                ai.source,
                ai.key,
                type_,
                type(value),
                value,
            )
            raise ValueError()
        return KaraboAttributoWithValue(ai, cast(T, data[ai.source][ai.key]))
    except KeyError:
        logger.warning(
            "Missing entry %s for source %s, key '%s' in the stream",
            attributo,
            ai.source,
            ai.key,
        )
        raise ValueError()


def generate_train_content(
    attributi: KaraboAttributi, data: KaraboData
) -> Optional[TrainContentDict]:
    try:
        return TrainContentDict(
            number=attributo2karabo(attributi, data, "run", "number", int),
            proposal_id=attributo2karabo(attributi, data, "run", "proposal_id", int),
            index=attributo2karabo(attributi, data, "run", "index", int),
            train_index_initial=attributo2karabo(
                attributi, data, "run", "train_index_initial", int
            ),
            timestamp_UTC_initial=attributo2karabo(
                attributi, data, "run", "timestamp_UTC_initial", str
            ),
            trains_in_run=attributo2karabo(
                attributi, data, "run", "trains_in_run", int
            ),
            status=RunStatus.RUNNING,
        )
    except ValueError:
        logger.exception("error extracting train content")
        return None


def compare_attributi_and_karabo_data(
    expected_attributi: KaraboExpectedAttributi,
    stream_content_: KaraboStreamKeys,
    ignore_entry: Dict[KaraboSource, FrozenSet[str]],
) -> List[str]:
    """Compare expected and available sources"""

    result: List[str] = []
    compare_missing_expected_attributi(expected_attributi, result, stream_content_)

    compare_extraneous_attributi(
        expected_attributi, ignore_entry, result, stream_content_
    )
    return result


def compare_extraneous_attributi(
    expected_attributi: KaraboExpectedAttributi,
    ignore_entry: Dict[KaraboSource, FrozenSet[str]],
    result: List[str],
    stream_content_: KaraboStreamKeys,
):
    for source, source_content in stream_content_.data.items():
        ignored = ignore_entry.get(source)

        # Ignore everything from this source
        if ignored is not None and ignored == {"IGNOREALL"}:
            continue

        if source not in expected_attributi:
            result.append(f"{source}: not requested")

        for key in source_content:
            if source in expected_attributi:
                if key not in expected_attributi[source]:

                    if ignored is not None:
                        if key not in ignored:
                            result.append(f"{source}//{key}: not requested")
                    else:
                        result.append(f"{source}//{key}: not requested")


def compare_missing_expected_attributi(
    expected_attributi: KaraboExpectedAttributi,
    result: List[str],
    stream_content_: KaraboStreamKeys,
):
    for source, source_content in expected_attributi.items():
        for key, attributo_and_group in source_content.items():
            attributo: KaraboAttributo = attributo_and_group["attributo"]
            group = attributo_and_group["group"]

            if source not in stream_content_.data:
                result.append(
                    f"[{group}.{attributo.identifier}] {source}: not available"
                )
            else:
                if key not in stream_content_.data[source]:
                    result.append(
                        f"[{group}.{attributo.identifier}] {source}//{key}: not available"
                    )
                else:
                    logger.debug(f"[{group}.{attributo.identifier}] {source}//{key}")
