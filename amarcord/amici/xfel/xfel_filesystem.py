import logging

import extra_data
import numpy as np

from amarcord.amici.xfel.karabo_cache import KaraboCacheContent
from amarcord.amici.xfel.karabo_configuration import KaraboConfiguration
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_online import compute_statistics
from amarcord.amici.xfel.proposed_run import ProposedRun

logger = logging.getLogger(__name__)


class FileSystem2Attributo:
    def __init__(
        self, proposal_id: int, run_id: int, config: KaraboConfiguration
    ) -> None:

        self.proposal_id = proposal_id
        self.run_id = run_id

        self.attributi = config.attributi
        self.expected_attributi = config.expected_attributi

        # TO BE FIXED: hardcoded attributo
        # self.attributi["run"].append(
        #    explicitize_attributo(
        #        identifier="run_duration",
        #        source=None,
        #        key=None,
        #        description="Run duration",
        #        type="decimal",
        #        unit="seconds",
        #    )
        # )

        # open a run
        self.run = extra_data.open_run(proposal=self.proposal_id, run=self.run_id)

        self.cache: KaraboCacheContent = {}

    def extract_data(self, number_of_bunches: int = 0) -> KaraboCacheContent:
        """It does what it promises"""

        # TO BE FIXED: because of nonsense filling values the number of pulses must be known to properly compute average quantities
        # ideally, we can read this from `SPB_RR_SYS/MDL/BUNCH_PATTERN//sase1.nPulses.value`
        # However, what will happen if we have pulses on demand?
        # -- current decision is to use the horrible filling values
        # self._cache: for compatibility with `compute_statistics`, although here it would be better to avoid caching and compute each attributo (also to reduce memory consumption)

        # TO BE FIXED: needs a more abstract approach

        if number_of_bunches == 0:
            for group in self.attributi:
                for attributo in self.attributi[group].values():
                    identifier, source, key = (
                        attributo.identifier,
                        attributo.source,
                        attributo.key,
                    )

                    if identifier == "source_number_of_bunches":
                        unique_number_of_bunches = np.unique(
                            self.run.get_array(source, key).values
                        )

                        if unique_number_of_bunches.size > 1:
                            logger.warning(
                                "number of bunches is %s: %s, taking the first one",
                                unique_number_of_bunches.size,
                                unique_number_of_bunches,
                            )

                        number_of_bunches = unique_number_of_bunches[0]

                        logging.info("Number of bunches: %s", number_of_bunches)

        for group in self.attributi:
            for attributo in self.attributi[group].values():
                identifier, source, key = (
                    attributo.identifier,
                    attributo.source,
                    attributo.key,
                )

                if group == "run":
                    # run 'number' is the primary key, and 'index' also will not be touched
                    # `timestamp_UTC_initial` should probably come from myMdC
                    # all the other fields will be uploaded since it could happen that something went wrong online

                    if identifier == "train_index_initial":
                        self.attributi[group][identifier].value = self.run.train_ids[0]

                    elif identifier == "trains_in_run":
                        self.attributi[group][identifier].value = len(
                            self.run.train_ids
                        )

                    # this is removed because not needed anymore
                    # elif identifier == "run_duration":
                    #    self.attributi[group][position]["value"] = (
                    #        self.run.train_ids[-1] - self.run.train_ids[0]
                    #    ) / 10.0

                    continue

                if source in self.run.all_sources and key in self.run.keys_for_source(
                    source
                ):
                    logging.info("Found %s: %s//%s", identifier, source, key)

                    # pylint: disable=fixme
                    # FIXME: again to work with self._cache
                    if source not in self.cache:
                        self.cache[source] = {}

                    data = self.run.get_array(source, key).values

                    # pylint: disable=fixme
                    # FIXME: breaks abstraction
                    # slice away filling values
                    if identifier in (
                        "source_bunch_charge",
                        "XGM_XTD9_SASE1_pulse_energy",
                        "XGM_XTD9_SASE1_beam_position_x",
                        "XGM_XTD9_SASE1_beam_position_y",
                    ):
                        # FIXME: relies on alphabetic order, number_f_bunches must be known before entering the
                        # loop
                        if number_of_bunches == 0:
                            raise ValueError("number of bunches is not defined")

                        data = data[:, :number_of_bunches]

                    self.cache[source][key] = data

                else:
                    logging.warning("Not found %s: %s//%s", identifier, source, key)

        return self.cache

    def compute_statistics(self):
        _attributi: KaraboExpectedAttributi = {}
        for group in self.attributi:
            for attributo in self.attributi[group].values():
                if attributo.source not in _attributi:
                    _attributi[attributo.source] = {}

                _attributi[attributo.source][attributo.key] = {
                    "group": group,
                    "attributo": attributo,
                }

        compute_statistics(
            self.cache,
            _attributi,
            self.attributi,
            current_run=ProposedRun(self.run_id, self.proposal_id),
        )

        return _attributi
