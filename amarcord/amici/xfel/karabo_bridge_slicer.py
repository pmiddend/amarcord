import copy
import logging
from collections import deque
from typing import Any
from typing import Deque
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import cast

from amarcord.amici.xfel.karabo_action import KaraboAction
from amarcord.amici.xfel.karabo_action import KaraboAttributiUpdate
from amarcord.amici.xfel.karabo_action import KaraboRunEnd
from amarcord.amici.xfel.karabo_action import KaraboRunStart
from amarcord.amici.xfel.karabo_cache import KaraboCache
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_general import parse_weird_karabo_datetime
from amarcord.amici.xfel.karabo_online import KaraboAttributoWithValue
from amarcord.amici.xfel.karabo_online import RunStatus
from amarcord.amici.xfel.karabo_online import TrainContentDict
from amarcord.amici.xfel.karabo_online import compare_attributi_and_karabo_data
from amarcord.amici.xfel.karabo_online import compare_metadata_trains
from amarcord.amici.xfel.karabo_online import compute_statistics
from amarcord.amici.xfel.karabo_online import generate_train_content
from amarcord.amici.xfel.karabo_online import parse_configuration
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_stream_keys import karabo_stream_keys

logger = logging.getLogger(__name__)


class KaraboBridgeSlicer:
    def __init__(
        self,
        attributi_definition: Dict[str, Any],
        ignore_entry: Dict[KaraboSource, List[str]],
        **_kwargs: Dict[str, Any],
    ) -> None:

        # build the attributi dictionary
        self.initial_bridge_content: Optional[KaraboStreamKeys] = None
        self._ignore_entry: Dict[KaraboSource, FrozenSet[str]] = {
            k: frozenset(v) for k, v in ignore_entry.items()
        }

        self._attributi, self.expected_attributi = parse_configuration(
            attributi_definition
        )

        self._train_history: Deque[int] = deque()
        self.run_history: Dict[int, TrainContentDict] = {}
        self._current_run: Optional[int] = None

        # populate the cache
        self._cache = KaraboCache(self.expected_attributi)

    def get_attributi(self) -> KaraboExpectedAttributi:
        return self.expected_attributi

    def run_definer(
        self,
        data: KaraboData,
        metadata: Dict[str, Any],
        train_cache_size: int = 5,
        averaging_interval: int = 50,
    ) -> List[KaraboAction]:
        trainId = compare_metadata_trains(metadata)

        if self._train_history and trainId - 1 != self._train_history[-1]:
            logger.info(
                "missed train; last %s, current %s", self._train_history[-1], trainId
            )

        self._train_history.append(trainId)
        self._train_history.popleft()

        # logger.debug("Train %s", trainId)

        # inspect the run
        train_content = generate_train_content(self._attributi, data)

        if train_content is None:
            return []

        is_first_train = self.initial_bridge_content is None
        if self.initial_bridge_content is None:
            self.initial_bridge_content = karabo_stream_keys(data, metadata)
            for message in compare_attributi_and_karabo_data(
                self.expected_attributi, self.initial_bridge_content, self._ignore_entry
            ):
                logger.warning(message)

        # run index
        self._current_run = train_content["number"].value

        # run is running
        if train_content["trains_in_run"].value == 0:
            if is_first_train:
                logger.info("Daemon is starting in the middle of a run...")
            # We don't know the run yet, but it's too long gone
            if (
                self._current_run not in self.run_history
                and train_content["train_index_initial"].value
                < trainId - train_cache_size
            ):
                logger.info(
                    "Run is out of our time window (train id %s, train cache size %s, initial train index: %s), "
                    "ignoring it...",
                    trainId,
                    train_cache_size,
                    train_content["train_index_initial"].value,
                )
                return []

            logger.debug("Caching train %s", trainId)

            # cache the data
            self._cache.cache_train(
                data, self.expected_attributi, self.initial_bridge_content
            )

            # starting a new run...
            if self._current_run not in self.run_history:
                # We don't know if all previous runs have been closed. Better do that now.
                for v in self.run_history.values():
                    v["status"] = RunStatus.CLOSED

                # Add our new run (which will be running by construction above)
                self.run_history[self._current_run] = train_content.copy()

                logger.info(
                    "Run %s started with train %s at %s",
                    train_content["number"].value,
                    trainId,
                    train_content["timestamp_UTC_initial"].value,
                )

                # populate run attributi
                for item_ in train_content.values():
                    if not isinstance(item_, KaraboAttributoWithValue):
                        continue
                    item = cast(KaraboAttributoWithValue[Any], item_)
                    if item.attributo.type_ == "datetime":
                        result_value = parse_weird_karabo_datetime(item.value)
                    else:
                        result_value = item.value
                    self._attributi["run"][
                        item.attributo.identifier
                    ].value = result_value

                # logger.info(
                #     "start attributi: %s",
                #     "\n".join(
                #         f"{x.identifier}"
                #         for v in self._attributi.values()
                #         for x in v.values()
                #     ),
                # )
                return [
                    KaraboRunStart(
                        run_id=self._current_run,
                        proposal_id=self._attributi["run"]["proposal_id"].value,
                        attributi=copy.deepcopy(self._attributi),
                    )
                ]

            # Run still running, not enough frames for statistics received yet.
            if (
                averaging_interval != 0
                and self._cache.cached_events % averaging_interval != 0
            ):
                return []

            # Run still running, averages make sense now
            # update the average and send results to AMARCORD
            compute_statistics(
                self._cache.content,
                self.expected_attributi,
                self._attributi,
                self._current_run,
            )

            return [
                KaraboAttributiUpdate(
                    self._current_run,
                    proposal_id=self._attributi["run"]["proposal_id"].value,
                    attributi=copy.deepcopy(self._attributi),
                )
            ]

        # run is over or in progress when we start
        # run is in progress when we start
        if train_content["number"].value not in self.run_history:
            if is_first_train:
                logger.info(
                    "Daemon is starting with an already finished run %s, waiting for new run",
                    train_content["number"].value,
                )
            return []

        # Run is over and we already know that.
        if self.run_history[self._current_run]["status"] == RunStatus.CLOSED:
            return []

        # Run is over and that's news to us, then note final run state
        self.run_history[self._current_run]["trains_in_run"] = train_content[
            "trains_in_run"
        ]
        self.run_history[self._current_run]["status"] = RunStatus.CLOSED

        logger.info(
            "Run %s closed at %s: %s train(s)",
            train_content["number"].value,
            trainId,
            train_content["trains_in_run"].value,
        )

        # update the average one more time and send results to AMARCORD
        compute_statistics(
            self._cache.content,
            self.expected_attributi,
            self._attributi,
            self._current_run,
        )

        # reset the cache
        self._cache = KaraboCache(self.expected_attributi)

        # populate run attributi
        for item_ in train_content.values():
            if not isinstance(item_, KaraboAttributoWithValue):
                continue
            item = cast(KaraboAttributoWithValue[Any], item_)
            if item.attributo.type_ == "datetime":
                result_value = parse_weird_karabo_datetime(item.value)
            else:
                result_value = item.value
            self._attributi["run"][item.attributo.identifier].value = result_value

        return [
            KaraboRunEnd(
                self._current_run,
                proposal_id=self._attributi["run"]["proposal_id"].value,
                attributi=copy.deepcopy(self._attributi),
            )
        ]
