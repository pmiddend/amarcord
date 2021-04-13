# type: ignore
# pylint: skip-file
import copy
import logging
import os
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import karabo_bridge
import numpy as np
import yaml

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


class Statistics:
    def __init__(self) -> None:
        pass

    @staticmethod
    def call(method, data, **kwargs):

        if method == "arithmetic_mean":
            self.arithmetic_mean(data, **kwargs)

        elif method == "standard_deviation":
            self.standard_deviation(data, **kwargs)

        else:
            raise KeyError("Method {} not implemented".format(method))

    @staticmethod
    def arithmetic_mean(data, axis=0):
        return np.mean(data, axis=axis)

    @staticmethod
    def standard_deviation(data, axis=0):
        return np.std(data, axis=axis, ddof=1)


KaraboAttributi = Dict[str, List[Dict[str, Any]]]


@dataclass(frozen=True)
class KaraboAttributiUpdate:
    run_id: int
    attributi: KaraboAttributi


@dataclass(frozen=True)
class KaraboRunEnd:
    run_id: int
    attributi: KaraboAttributi


@dataclass(frozen=True)
class KaraboRunStartOrUpdate:
    run_id: int
    attributi: KaraboAttributi


KaraboAction = Union[KaraboAttributiUpdate, KaraboRunEnd, KaraboRunStartOrUpdate]


class KaraboBridgeSlicer:
    def __init__(
        self,
        attributi_definition: Dict[str, Any],
        ignore_entry: Dict[str, List[str]],
        **kwargs: Dict[str, Any]
    ) -> None:

        # build the attributi dictionary
        self._attributi: KaraboAttributi
        self._attributi, self.attributi = self._parse_configuration(
            attributi_definition
        )

        self._cache, self.statistics = self._initialize_cache()
        self._ignore_entry = ignore_entry

        self._train_history = []
        self._current_run = None
        self.karabo_bridge_content = None
        self.run_history = {}

        # sanity check
        self._sanity_get_all_trains = 0

    def get_attributi(self) -> Dict[str, List[Dict[str, Any]]]:
        return self._attributi

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def _attributo2karabo(self, group: str, attributo: str) -> Tuple[str, str]:
        """Karabo source and key from `attributo` identifier

        Args:
            group (str): Group hosting the `attributo`
            attributo (str): `attributo` identifier

        Returns:
            Tuple[str, str]: Karabo source and key
        """

        for ai in self._attributi[group]:
            if ai["identifier"] == attributo:

                return ai["source"], ai["key"]

    def _explicitize_attributo(
        self,
        identifier: str,
        source: str,
        key: str,
        description: str = None,
        store: bool = True,
        action: str = "compute_arithmetic_mean",
        unit: str = None,
        filling_value: Any = None,
    ) -> Dict[str, Any]:
        """Explicitize an `attributo`, i.e. fill default values if needed.

        Args:
            identifier (str): `attributo` identifier
            source (str): EuXFEL source
            key (str): Value to extract
            description (str, optional): `attributo` description. Defaults to None.
            store (bool, optional): Whether to store the value. Defaults to True.
            action (str, optional): Either "compute_arithmetic_mean", "compute_standard_deviation", "check_if_constant" or "store_last". Defaults to "compute_arithmetic_mean".
            unit (str, optional): Unit of measurement. Defaults to None.
            filling_value (Any, optional): Filling value in case a source is missing. Defaults to None.

        Raises:
            ValueError: If action is different from "compute_statistics", "check_if_constant" or "store_last"

        Returns:
            (Dict[str, Any]): The `attributo`
        """
        attributo = dict(set(locals().items()) - set({"self": self}.items()))

        action_choice = [
            "compute_arithmetic_mean",
            "compute_standard_deviation",
            "check_if_constant",
            "store_last",
        ]
        if action not in action_choice:
            raise ValueError(
                "Action must be either '{}'...".format("' or '".join(action_choice))
            )

        return attributo

    def _parse_configuration(
        self, configuration: Dict[str, Any]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Parse the configuration file

        Args:
            configuration (Dict[str, Any]): The configuration

        Raises:
            TypeError: If the attributo syntax is wrong

        Returns:
            Dict[str, List[Dict[str, Any]]], Dict[str, List[str]]: A dictionary of attributi and one with expected Karabo keywords
        """
        entry: Dict[str, List[Dict[str, Any]]] = {}
        karabo_expected_entry: Dict[List[str]] = {}

        for (gi, gi_content,) in configuration.items():
            source = None

            for (ai, ai_content,) in gi_content.items():

                # source can be set globally, for the entire group
                if ai == "source":
                    source = ai_content

                if isinstance(ai_content, dict):
                    attributo = {}

                    if gi not in entry.keys():
                        entry[gi] = []

                    if source is not None:
                        attributo["source"] = source

                    # fill the attributo
                    for ki, vi in ai_content.items():
                        attributo[ki] = vi

                    # add the attributo
                    try:
                        entry[gi].append(
                            self._explicitize_attributo(identifier=ai, **attributo)
                        )

                    except TypeError:
                        raise TypeError(
                            "Wrong attributo definition in {}//{}".format(gi, attributo)
                        )

        # build the corresponding Karabo bridge expected entry
        for group_name, group in entry.items():
            for attributo in group:
                if attributo["source"] not in karabo_expected_entry:
                    karabo_expected_entry[attributo["source"]] = {
                        attributo["key"]: {**attributo, "group": group_name}
                    }
                else:
                    karabo_expected_entry[attributo["source"]].update(
                        {attributo["key"]: {**attributo, "group": group_name}}
                    )

        return entry, karabo_expected_entry

    def _initialize_cache(
        self,
    ) -> Tuple[Dict[str, Dict[str, List]], Dict[str, Dict[str, Any]]]:
        """Initialize arrays holding data

        Returns:
            Tuple[Dict[str, Dict[str, List]], Dict[str, Dict[str, Any]]]: The cache
        """
        cache, statistics = {}, {"arithmetic_mean": {}, "standard_deviation": {}}

        for source, source_content in self.attributi.items():
            cache[source] = {li: [] for li in source_content}

            for ki in statistics:
                statistics[ki][source] = {li: None for li in source_content}

        return cache, statistics

    def _stream_content(
        self, data: Dict[str, Any], metadata: Dict[str, Any]
    ) -> Dict[str, List[str]]:
        """Navigate the stream from the Karabo bridge

        Args:
            stream (Dict[str, Any]): The Karabo bridge stream

        Returns:
            Dict[str, List[str]]: The stream content
        """
        _data, _metadata = {}, {}

        def extractor(stream):
            container = {}

            for si, si_content in stream.items():
                container[si] = []

                for ki in si_content.keys():
                    container[si].append(ki)

            return container

        return {"data": extractor(data), "metadata": extractor(metadata)}

    def _compare_attributi_and_karabo_data(self, verbose=True) -> None:
        """Compare expected and available sources

        Args:
            verbose (bool, optional): Be verbose. Defaults to True.
        """

        if verbose:
            logging.info("Requested and missing entries:")

            for source, source_content in self.attributi.items():
                for key, attributo in source_content.items():

                    if source not in self.karabo_bridge_content["data"]:
                        logging.warning(
                            "  [{}.{}] {}: not available".format(
                                attributo["group"], attributo["identifier"], source
                            )
                        )
                    else:
                        if key not in self.karabo_bridge_content["data"][source]:
                            logging.warning(
                                "  [{}.{}] {}//{}: not available".format(
                                    attributo["group"],
                                    attributo["identifier"],
                                    source,
                                    key,
                                )
                            )
                        else:
                            logging.info(
                                "  [{}.{}] {}//{}".format(
                                    attributo["group"],
                                    attributo["identifier"],
                                    source,
                                    key,
                                )
                            )

            for source, source_content in self.karabo_bridge_content["data"].items():
                if source not in self.attributi:
                    logging.warning("  {}: not requested".format(source))

                for key in source_content:
                    if source in self.attributi:
                        if key not in self.attributi[source]:

                            if source in self._ignore_entry:
                                if key not in self._ignore_entry[source]:

                                    logging.warning(
                                        "  {}//{}: not requested".format(source, key,)
                                    )

    def _compare_metadata_trains(self, metadata: Dict[str, Any]) -> int:
        """Checks if Karabo devices are synchronized

        Args:
            metadata (Dict[str, Any]): Metadata from the Karabo bridge

        Raises:
            ValueError: One or more devices are desynchronized

        Returns:
            int: Current trainId
        """
        trainId = set(
            [
                source_content["timestamp.tid"]
                for source, source_content in metadata.items()
            ]
        )

        if len(trainId) > 1:
            raise ValueError("Karabo devices desynchronized")

        return tuple(trainId)[0]

    def cache_train(self, data: Dict[str, Any], metadata: Dict[str, Any]) -> None:
        """Cache a given train from the Karabo bridge

        Args:
            data (Dict[str, Any]): Data from the bridge
            metadata (Dict[str, Any]): Metadata from the bridge
        """

        if self.karabo_bridge_content is None:
            self.karabo_bridge_content = self._stream_content(data, metadata)

            self._compare_attributi_and_karabo_data()

        # cache data
        for source, source_content in self.attributi.items():
            if source in self.karabo_bridge_content["data"]:
                for key in source_content.keys():
                    if key in self.karabo_bridge_content["data"][source]:

                        try:
                            value = data[source][key]
                        except KeyError:
                            value = self.attributi[source][key]["filling_value"]

                        if self.attributi[source][key]["action"] == "store_last":
                            self._cache[source][key] = value
                        else:
                            self._cache[source][key].append(value)

    def cache_next_train(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get and cache the next train from the Karabo bridge

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: Tuple[Dict[str, Any], Dict[str, Any]]: data, metadata
        """

        # get the next train
        data, metadata = self.get_next_train()

        # cache data
        self.cache_train(data, metadata)

        return data, metadata

    def _compute_statistics(self) -> None:
        """Compute statistics"""

        def remove_filling_values(data, filling_value):
            container = []
            removed = 0

            for entry in data:
                if not hasattr(entry, "__len__"):
                    # remove filling value, which must be a scalar
                    if entry == filling_value:
                        removed += 1

                        continue

                container.append(entry)

            return container, removed

        for source in self._cache.keys():
            for key in self._cache[source].keys():

                # remove filling values
                cached_data, removed = remove_filling_values(
                    self._cache[source][key],
                    self.attributi[source][key]["filling_value"],
                )

                if removed:
                    logging.warning(
                        "{}//{}: removed {} entries".format(source, key, removed)
                    )

                # compute statistics
                if (
                    self.attributi[source][key]["action"] == "compute_arithmetic_mean"
                ) or (
                    self.attributi[source][key]["action"]
                    == "compute_standard_deviation"
                ):
                    reduced_value = Statistics.call(
                        self.attributi[source][key]["action"], cached_data
                    )
                    self.attributi[source][key]["value"] = reduced_value

                    logging.debug(
                        "{} on {}//{}:\n  intial dataset: {}\n  reduced value: {}".format(
                            source, key, self._cache[source][key], reduced_value
                        )
                    )

                # check if values are constant
                elif self.attributi[source][key]["action"] == "check_if_constant":
                    if len(set(cached_data)):
                        logging.warning(
                            "{}//{}: not constant over run {}".format(
                                source, key, self._current_run
                            )
                        )

    def _sanity_check_get_all_trains(self):
        """Check if we get all trains"""
        for vi in range(self._sanity_get_all_trains + 1, len(self._train_history)):
            if self._train_history[vi] - self._train_history[vi - 1] > 1:
                logging.warning(
                    "Missed train {}".format(self._train_history[vi - 1] + 1)
                )

                self._sanity_get_all_trains = len(self._train_history) - 1

    def _sanity_check_runs_are_closed(self):
        """Check if a run that should be closed is still running"""
        running = []

        for ki, vi in self.run_history.items():
            if vi["status"] == "running":
                running.append(ki)

        if len(running) > 1:
            logging.warn(
                "Multiple runs runnig {}. Forcefully closing run {}".format(
                    running, running[0]
                )
            )

            self.run_history[running[0]]["status"] = "closed"

    def run_definer(
        self,
        data: Dict[str, Any],
        metadata: Dict[str, Any],
        train_cache_size: int = 5,
        averaging_interval: int = 10,
    ) -> List[KaraboAction]:
        """Defines a run

        Args:
            data (Dict[str, Any]): Data from the Karabo bridge
            metadata (Dict[str, Any]): Data from the Karabo bridge
            train_cache_size (int, optional): In case trains are missed, this must be bigger than 0. Defaults to 5.
            averaging_interval (int, optional): Average updating frequency. Defaults to 10.
        """
        trainId = self._compare_metadata_trains(metadata)

        self._train_history.append(trainId)

        logging.debug("Train {}".format(trainId))

        # check if we get all trains
        self._sanity_check_get_all_trains()

        # inspect the run
        train_content = {}

        for attributo in [
            "index",
            "train_index_initial",
            "timestamp_UTC_initial",
            "trains_in_run",
        ]:
            source, key = self._attributo2karabo("run", attributo)

            try:
                train_content[attributo] = data[source][key]
            except KeyError:
                logging.warning("Missing entry '{}' in the stream".format(attributo))

        result: List[KaraboAction] = []

        # run index
        self._current_run = train_content["index"]

        # run is running
        if not train_content["trains_in_run"]:

            # starting a new run...
            if train_content["train_index_initial"] <= trainId + train_cache_size:
                self.run_history[self._current_run] = {**train_content} + {
                    "status": "running",
                }

                result.append(
                    KaraboRunStartOrUpdate(
                        self._current_run, copy.deepcopy(self._attributi)
                    )
                )

                # are all the runs closed?
                self._sanity_check_runs_are_closed()

                logging.info(
                    "Run {index} started at {timestamp_UTC_initial}}".format(
                        **train_content,
                    )
                )

            # update the average and send results to AMARCORD
            if (not len(self._cache)) and (not len(self._cache) % averaging_interval):
                self._compute_statistics()
                return [
                    KaraboAttributiUpdate(
                        self._current_run, copy.deepcopy(self._attributi)
                    )
                ]

        # run is over or in progress when we start
        else:
            # run is in progress when we start
            if train_content["index"] not in self.run_history:

                # we are really in the middle of the run if "trains_in_run" == 0
                if not train_content["trains_in_run"]:
                    self.run_history[self._current_run] = {**train_content} + {
                        "status": "running",
                    }

                    result.append(
                        KaraboRunStartOrUpdate(
                            self._current_run, train_content["index"]
                        )
                    )
                # the runId value is still here even if the run is over
                # we don't want to record anything, we check the stream and exit here
                else:
                    if self.karabo_bridge_content is None:
                        self.karabo_bridge_content = self._stream_content(
                            data, metadata
                        )

                        self._compare_attributi_and_karabo_data()

                    return result

            # run is over
            if self.run_history[train_content["index"]]["status"] != "finished":
                self.run_history[self._current_run]["trains_in_run"] = train_content[
                    "trains_in_run"
                ]
                self.run_history[self._current_run]["status"] = "closed"

                logging.info(
                    "Run {index} completed with {trains_in_run} trains".format(
                        **train_content
                    )
                )

                # update the average one more time and send results to AMARCORD
                self._compute_statistics()

                # reset the cache
                self._initialize_cache()

                result.append(
                    KaraboRunEnd(self._current_run, copy.deepcopy(self._attributi))
                )
                return result
        return []


if __name__ == "__main__":

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

    config = load_configuration("./config.yml")

    # instantiate the Karabo bridge client
    client_endpoint = config["Karabo_bridge"]["client_endpoint"]
    client = karabo_bridge.Client(client_endpoint)

    logging.info("Connected to the Karabo bridge at {}\n".format(client_endpoint))

    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    while True:
        data, metadata = client.next()

        karabo_data.run_definer(data, metadata)
