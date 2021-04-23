import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Union

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_key import KaraboKey
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

                try:
                    value = data[source][key]
                except KeyError:
                    value = expected["attributo"].filling_value

                if not match_type(expected["attributo"].type_, value):
                    logger.warning(
                        '%s//%s: expected value of type %s, got "%s" (type %s)',
                        source,
                        key,
                        expected["attributo"].type_,
                        value,
                        type(value),
                    )
                    continue

                action = expected["attributo"].action
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
