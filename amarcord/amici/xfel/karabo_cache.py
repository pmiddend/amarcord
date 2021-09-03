import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Union

import numpy as np

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_general import parse_weird_karabo_datetime
from amarcord.amici.xfel.karabo_key import KaraboKey
from amarcord.amici.xfel.karabo_processor import KaraboProcessor
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_value import KaraboValue

logger = logging.getLogger(__name__)


class KaraboCache:
    def __init__(self, expected_attributi: KaraboExpectedAttributi) -> None:
        self.cached_events = 0
        self.content: KaraboCacheContent = {}
        self.statistics: Dict[str, Dict[str, Any]] = {
            "arithmetic_mean": {},
            "standard_deviation": {},
        }

        for source, source_content in expected_attributi.items():
            self.content[source] = {li: [] for li in source_content}

            # probably a good refactoring (use values() instead of grabbing via ki)
            # but too lazy to change right now
            # pylint: disable=consider-using-dict-items
            for ki in self.statistics:
                self.statistics[ki][source] = {li: None for li in source_content}

    def cache_train(
        self,
        data: KaraboData,
        expected_attributi: KaraboExpectedAttributi,
        bridge_content: KaraboStreamKeys,
    ) -> None:
        for source, source_content in expected_attributi.items():
            if source not in bridge_content.data:
                continue
            for key in source_content:
                if key not in bridge_content.data[source]:
                    continue

                expected = expected_attributi[source][key]
                attributo = expected["attributo"]

                try:
                    value = data[source][key]
                except KeyError:
                    value = attributo.filling_value

                expected_type = (
                    attributo.karabo_type
                    if attributo.karabo_type is not None
                    else attributo.type_
                )
                if not match_type(expected_type, value):
                    logger.warning(
                        '%s//%s: expected value of type %s, got "%s" (type %s)',
                        source,
                        key,
                        expected_type,
                        value,
                        type(value),
                    )
                    continue

                if attributo.processor == KaraboProcessor.TO_CONSTANT:
                    if not isinstance(value, list) and (
                        not isinstance(value, np.ndarray) or len(value.shape) != 1
                    ):
                        logger.warning(
                            '%s//%s: cannot apply processor "%s" on attributo of type %s, not implemented yet',
                            source,
                            key,
                            attributo.processor,
                            type(value),
                        )
                        continue
                    unique_values = set(value)  # type: ignore
                    if not unique_values:
                        logger.warning(
                            '%s//%s: cannot apply processor "to constant" to empty list, not defined',
                            source,
                            key,
                        )
                        continue
                    if len(unique_values) > 1:
                        logger.warning(
                            "%s//%s: expected constant values in list, got (excerpt) %s",
                            source,
                            key,
                            list(unique_values)[0:5],
                        )
                    value = value[0]  # type: ignore
                elif attributo.processor == KaraboProcessor.ARITHMETIC_MEAN:
                    if not isinstance(value, list) and (
                        not isinstance(value, np.ndarray) or len(value.shape) != 1
                    ):
                        logger.warning(
                            '%s//%s: cannot apply processor "%s" on attributo of type %s, not implemented yet',
                            source,
                            key,
                            attributo.processor,
                            type(value),
                        )
                        continue
                    value = np.mean(value)

                # Do any sort of type conversion here
                if attributo.type_ == "datetime":
                    assert isinstance(value, str)
                    # It's a bit weird here, only works for datetime currently
                    value = parse_weird_karabo_datetime(value)  # type: ignore

                action = attributo.action
                if action == KaraboAttributoAction.STORE_LAST:
                    self.content[source][key] = value
                else:
                    content_list = self.content[source][key]
                    assert isinstance(
                        content_list, list
                    ), f"with action {action} we expect a list in cache, got {(type(content_list))}"
                    # don't know why mypy doesn't "get it", maybe our isinstance above is slightly wrong
                    self.content[source][key].append(value)  # type: ignore
        self.cached_events += 1


def match_type(type_: str, value: KaraboValue) -> bool:
    if type_ == "decimal" and not isinstance(value, (float, int)):
        return False
    if type_ == "int" and not isinstance(value, int):
        return False
    if type_ == "datetime" and not isinstance(value, str):
        return False
    return True


KaraboCacheContent = Dict[
    KaraboSource, Dict[KaraboKey, Union[KaraboValue, List[KaraboValue]]]
]
