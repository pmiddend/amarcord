import logging
from dataclasses import dataclass

# pylint: disable=wrong-import-order
from enum import Enum

# pylint: disable=wrong-import-order
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

from amarcord.amici.xfel.karabo_attributi import KaraboAttributi
from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_cache import KaraboCacheContent
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_statistics import KaraboStatistics
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_value import KaraboValue
from amarcord.amici.xfel.proposed_run import ProposedRun

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
    current_run: ProposedRun,
) -> None:
    for source, source_content in cache.items():
        for key in source_content:

            attributo = expected_attributi[source][key]["attributo"]

            # remove filling values
            cached_value = cache[source][key]
            cached_data, removed = _remove_filling_values(
                cached_value,
                attributo.filling_value,
            )

            if removed:
                logger.warning(f"{source}//{key}: removed {removed} entries")

            # compute statistics
            if attributo.action in (
                KaraboAttributoAction.COMPUTE_STANDARD_DEVIATION,
                KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
            ):
                reduced_value = KaraboStatistics.call(
                    attributo.action,
                    cached_data,
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
                unique_value, unique_index = np.unique(
                    np.hstack(cached_data) if not cached_data else cached_data,  # type: ignore
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


def _remove_filling_values(
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
