from typing import Any, List, Tuple, Dict

import os
import sys
import yaml
import logging
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

        self._cache, self._trainId = self._initialize_cache()

        # inform AMARCORD
        #

        # instantiate the Karabo bridge client
        self.client_endpoint = client_endpoint
        self._client = karabo_bridge.Client(self.client_endpoint)

        self.karabo_bridge_content = None

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
        action: str = "average",
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
            action (str, optional): Either "average", "check_if_constant" or "store_last". Defaults to "average".
            unit (str, optional): Unit of measurement. Defaults to None.
            filling_value (Any, optional): Filling value in case a source is missing. Defaults to None.

        Raises:
            ValueError: If action is different from "average", "check_if_constant" or "store_last"

        Returns:
            (Dict[str, Any]): The `attributo`
        """
        attributo = dict(set(locals().items()) - set({"self": self}.items()))

        action_choice = ["average", "check_if_constant", "store_last"]
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

    def _initialize_cache(self) -> Tuple[Dict[str, Dict[str, List]], int]:
        """Initialize arrays holding data

        Returns:
            Tuple[Dict[str, Dict[str, List]], List[int]]: The cache and tranId array
        """
        cache = {}

        for source, source_content in self.attributi.items():
            cache[source] = {li: [] for li in source_content}

        return cache, []

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

    def cache_train(
        self, data: Dict[str, Any], metadata: Dict[str, Any], verbose=True
    ) -> None:
        """Cache a given train from the Karabo bridge

        Args:
            data (Dict[str, Any]): Data from the bridge
            metadata (Dict[str, Any]): Metadata from the bridge
            verbose (bool, optional): Be verbose. Defaults to True.
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

        trainId = self._compare_metadata_trains(metadata)
        self._trainId.append(trainId)
        size = len(self._trainId)

        if verbose:
            if not size % 10:
                logging.info("Train {}: cached {} events".format(trainId, size))

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

    def run_slicer(self):

        data, _ = self.cache_next_train()

        # tutto questo in un loop
        # variabili in un dict

        # run index
        source, key = self._attributo2karabo("run", "index")
        index = data[source][key]

        # first train in the run
        source, key = self._attributo2karabo("run", "train_index_initial")
        train_index_initial = data[source][key]

        # starting time
        source, key = self._attributo2karabo("run", "timestamp_UTC_initial")
        timestamp_UTC_initial = data[source][key]

        # is the run complete?
        source, key = self._attributo2karabo("run", "trains_in_run")
        trains_in_run = data[source][key]

        if trains_in_run > 0:
            logging.info(
                "Run {index} completed: {trains_in_run} trains starting at {timestamp_UTC_initial} from index {train_index_initial}".format(
                    index, trains_in_run, timestamp_UTC_initial, train_index_initial,
                )
            )

            # reset cache?

        else:

            # running average?
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
        karabo_data.run_slicer()
