from typing import Any, List, Tuple, Dict

import os
import sys
import yaml
import logging
import numpy as np
import sqlalchemy as sa
import karabo_bridge

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


class KaraboBridge:
    def __init__(
        self,
        client_endpoint: str,
        attributi_definition: Dict[str, Any],
        **kwargs: Dict[str, Any]
    ) -> None:

        # build the attributi dictionary
        self._attributi, self.attributi = self._parse_configuration(
            attributi_definition
        )

        self._cache, self.statistics = self._initialize_cache()

        # inform AMARCORD
        #

        # instantiate the Karabo bridge client
        self.client_endpoint = client_endpoint
        self._client = karabo_bridge.Client(self.client_endpoint)

        self.karabo_bridge_content = None
        self._train_history = []
        self.run_history = {}

        logging.info("Connected to the Karabo bridge at {}\n".format(client_endpoint))

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
        action: str = "compute_statistics",
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
            action (str, optional): Either "compute_statistics", "check_if_constant" or "store_last". Defaults to "compute_statistics".
            unit (str, optional): Unit of measurement. Defaults to None.
            filling_value (Any, optional): Filling value in case a source is missing. Defaults to None.

        Raises:
            ValueError: If action is different from "compute_statistics", "check_if_constant" or "store_last"

        Returns:
            (Dict[str, Any]): The `attributo`
        """
        attributo = dict(set(locals().items()) - set({"self": self}.items()))

        action_choice = ["compute_statistics", "check_if_constant", "store_last"]
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
            Dict[str, List[Dict[str, Any]]]: A dictionary of attributi and one with expected Karabo keywords
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
                            "Wrong attributo definition in {}::{}".format(gi, attributo)
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
                        logging.warn(
                            "  [{}.{}] {}: not available".format(
                                attributo["group"], attributo["identifier"], source
                            )
                        )
                    else:
                        if key not in self.karabo_bridge_content["data"][source]:
                            logging.warn(
                                "  [{}.{}] {}::{}: not available".format(
                                    attributo["group"],
                                    attributo["identifier"],
                                    source,
                                    key,
                                )
                            )
                        else:
                            logging.info(
                                "  [{}.{}] {}::{}".format(
                                    attributo["group"],
                                    attributo["identifier"],
                                    source,
                                    key,
                                )
                            )

            for source, source_content in self.karabo_bridge_content["data"].items():
                if source not in self.attributi:
                    logging.warn("  {}: not requested".format(source))

                for key in source_content:
                    if source in self.attributi:
                        if key not in self.attributi[source]:
                            logging.warn("  {}::{}: not requested".format(source, key,))

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

    def get_next_train(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get the next train from the Karabo bridge

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: Tuple[Dict[str, Any], Dict[str, Any]]: data, metadata
        """

        return self._client.next()

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

    def _compute_statistics(self):
        """Compute statistics
        """

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

                if self.attributi[source][key]["action"] == "compute_statistics":
                    cached_data, removed = remove_filling_values(
                        self._cache[source][key],
                        self.attributi[source][key]["filling_value"],
                    )

                    if removed:
                        logging.info(
                            "{}//{}: removed {} entries".format(source, key, removed)
                        )

                    # compute statistics
                    self.statistics["arithmetic_mean"] = np.mean(cached_data, axis=0)
                    self.statistics["standard_deviation"] = np.std(
                        cached_data, axis=0, ddof=1
                    )

    def run_definer(self, averaging_interval=10):
        """Defines a run

        Args:
            averaging_interval (int, optional): [description]. Defaults to 10.
        """

        # get a train
        data, metadata = self.get_next_train()
        trainId = self._compare_metadata_trains(metadata)

        self._train_history.append(trainId)

        logging.debug("Train {}".format(trainId))

        # inspect it
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
                logging.warn("Missing entry '{}' in the stream".format(attributo))

        # run is running
        if not train_content["trains_in_run"]:

            # starting a new run...
            if train_content["train_index_initial"] == trainId:
                index = train_content["index"]

                self.run_history[index] = {**train_content} + {
                    "status": "running",
                }

                logging.info(
                    "Run {index} started at {timestamp_UTC_initial}}".format(
                        **train_content,
                    )
                )

        # run is over
        else:
            if self.run_history[train_content["index"]]["status"] == "finished":
                index = train_content["index"]

                self.run_history[index]["trains_in_run"] = train_content[
                    "trains_in_run"
                ]
                self.run_history[index]["status"] = "finished"

                return

            logging.info(
                "Run {index} completed with {trains_in_run} trains".format(
                    **train_content
                )
            )

            # update the average
            self._compute_statistics()

            # reset the cache
            self._initialize_cache()

            return

        # update the average
        if not len(self._cache) % averaging_interval:
            self._compute_statistics()

    def _define_AMARCORD_table(table, foreign_key="run_id"):
        pass


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

    karabo_data = KaraboBridge(**config["Karabo_bridge"])

    while True:
        karabo_data.run_definer()
