import copy
import logging
from typing import Any
from typing import Dict
from typing import FrozenSet
from typing import List
from typing import Optional
from typing import Set

from amarcord.amici.xfel.karabo_action import KaraboAction
from amarcord.amici.xfel.karabo_action import KaraboAttributiUpdate
from amarcord.amici.xfel.karabo_action import KaraboRunEnd
from amarcord.amici.xfel.karabo_action import KaraboRunStart
from amarcord.amici.xfel.karabo_cache import KaraboCache
from amarcord.amici.xfel.karabo_configuration import KaraboConfiguration
from amarcord.amici.xfel.karabo_data import KaraboData
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_online import compare_attributi_and_karabo_data
from amarcord.amici.xfel.karabo_online import compare_metadata_trains
from amarcord.amici.xfel.karabo_online import compute_statistics
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole
from amarcord.amici.xfel.karabo_stream_keys import KaraboStreamKeys
from amarcord.amici.xfel.karabo_stream_keys import karabo_stream_keys
from amarcord.amici.xfel.proposed_run import ProposedRun

logger = logging.getLogger(__name__)


def retrieve_int_attributo_with_role(
    expected_attributi: KaraboExpectedAttributi,
    data: KaraboData,
    role: KaraboSpecialRole,
) -> int:
    attributo = next(
        iter(
            y["attributo"]
            for x in expected_attributi.values()
            for y in x.values()
            if y["attributo"].role == role
        ),
        None,
    )
    if attributo is None:
        raise ValueError(f"couldn't find attributo for special role {role}")
    result = data[attributo.source].get(attributo.key, None)
    if result is None:
        raise ValueError(
            f"couldn't find value for special role {role}, attributo id {attributo.identifier}"
        )
    if not isinstance(result, int):
        raise ValueError(
            f"special value attributo {attributo.identifier} with role {role} is not int but {type(result)}: {result}"
        )
    return result


class KaraboBridgeSlicer:
    def __init__(self, configuration: KaraboConfiguration) -> None:

        # build the attributi dictionary
        self.initial_bridge_content: Optional[KaraboStreamKeys] = None
        self._ignore_entry: Dict[KaraboSource, FrozenSet[str]] = {
            k: frozenset(v) for k, v in configuration.ignore_entry.items()
        }

        self._attributi = configuration.attributi
        self.expected_attributi = configuration.expected_attributi

        self._last_train: Optional[int] = None
        self._run_history: Set[ProposedRun] = set()
        self._last_closed_run: Optional[ProposedRun] = None

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

        if self._last_train is not None and trainId - 1 != self._last_train:
            logger.info("missed train; last %s, current %s", self._last_train, trainId)

        is_first_train = self.initial_bridge_content is None
        if self.initial_bridge_content is None:
            self.initial_bridge_content = karabo_stream_keys(data, metadata)
            for message in compare_attributi_and_karabo_data(
                self.expected_attributi, self.initial_bridge_content, self._ignore_entry
            ):
                logger.warning(message)

        current_run = ProposedRun(
            run_id=retrieve_int_attributo_with_role(
                self.expected_attributi, data, KaraboSpecialRole.RUN_NUMBER
            ),
            proposal_id=retrieve_int_attributo_with_role(
                self.expected_attributi, data, KaraboSpecialRole.PROPOSAL_ID
            ),
        )

        trains_in_run = retrieve_int_attributo_with_role(
            self.expected_attributi, data, KaraboSpecialRole.TRAINS_IN_RUN
        )

        # run is running
        if trains_in_run == 0:
            if is_first_train:
                logger.info(
                    "Daemon is starting in the middle of run %s...", current_run
                )

            initial_train = retrieve_int_attributo_with_role(
                self.expected_attributi, data, KaraboSpecialRole.TRAIN_INDEX_INITIAL
            )
            # We don't know the run yet, but it's too long gone
            if (
                current_run not in self._run_history
                and initial_train < trainId - train_cache_size
            ):
                logger.info(
                    "Run %s is out of our time window (train id %s, train cache size %s, initial train index: %s), "
                    "ignoring it...",
                    current_run,
                    trainId,
                    train_cache_size,
                    initial_train,
                )
                return []

            # cache the data
            self._cache.cache_train(
                data, self.expected_attributi, self.initial_bridge_content
            )

            # starting a new run...
            if current_run not in self._run_history:
                # Add our new run (which will be running by construction above)
                self._run_history.add(current_run)

                logger.info("Run %s started with train %s", current_run, trainId)

                return [
                    KaraboRunStart(
                        run_id=current_run.run_id,
                        proposal_id=current_run.proposal_id,
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
            # This uses the cache to set values inside _attributi (and doesn't update the cache)
            compute_statistics(
                self._cache.content,
                self.expected_attributi,
                self._attributi,
                current_run,
            )

            return [
                KaraboAttributiUpdate(
                    run_id=current_run.run_id,
                    proposal_id=current_run.proposal_id,
                    attributi=copy.deepcopy(self._attributi),
                )
            ]

        # run is over or in progress when we start
        # run is in progress when we start
        if current_run not in self._run_history:
            if is_first_train:
                logger.info(
                    "Daemon is starting with an already finished run %s, waiting for new run",
                    current_run,
                )
            return []

        # Run is over and we already know that.
        if self._last_closed_run == current_run:
            return []

        # Run is over and that's news to us, then note final run state
        self._last_closed_run = current_run

        logger.info(
            "Run %s closed at %s: %s train(s)",
            current_run,
            trainId,
            trains_in_run,
        )

        # update the average one more time and send results to AMARCORD
        compute_statistics(
            self._cache.content,
            self.expected_attributi,
            self._attributi,
            current_run,
        )

        # reset the cache
        self._cache = KaraboCache(self.expected_attributi)

        return [
            KaraboRunEnd(
                run_id=current_run.run_id,
                proposal_id=current_run.proposal_id,
                attributi=copy.deepcopy(self._attributi),
            )
        ]
