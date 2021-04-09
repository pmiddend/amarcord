from typing import Any, List, Tuple, Dict

import os
import sys
import yaml
import logging
import karabo_bridge

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s [%(module)s] %(message)s",
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

        self.cache = self._initialize_cache()

        # inform AMARCORD
        #

        # instantiate the Karabo bridge client
        self.client_endpoint = client_endpoint
        self._client = karabo_bridge.Client(self.client_endpoint)

        self.karabo_bridge_content = None

        logging.info("Connected to the Karabo bridge at {}".format(client_endpoint))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

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
            action (str, optional): Either average or check_if_constant. Defaults to "average".
            unit (str, optional): Unit of measurement. Defaults to None.
            filling_value (Any, optional): Filling value in case a source is missing. Defaults to None.

        Raises:
            ValueError: If action is different from "average" or "check_if_constant"

        Returns:
            (Dict[str, Any]): The `attributo`
        """
        attributo = dict(set(locals().items()) - set({"self": self}.items()))

        action_choice = ["average", "check_if_constant"]
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

    def _initialize_cache(self) -> Dict[str, Dict[str, List]]:
        """Initialize arrays holding data

        Returns:
            Dict[str, Dict[str, List]]: The cache
        """
        cache = {}

        for source, source_content in self.attributi.items():
            cache[source] = {li: [] for li in source_content}

        return cache

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

    def _compare_attributi_and_karabo_data(self, verbose=True):

        missing_source = set(self.attributi) ^ set(self.karabo_bridge_content["data"])

        # to be sure we are not missing anything
        if verbose:
            logging.info("Requested entries:")

            for source, source_content in self.attributi.items():
                for key in source_content.keys():
                    logging.info("  {}::{}".format(source, key))

    def cache_next_train(self) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get and cache the next train from the Karabo bridge

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: Tuple[Dict[str, Any], Dict[str, Any]]: data, metadata
        """

        # get the next train
        data, metadata = self._client.next()

        if self.karabo_bridge_content is None:
            self.karabo_bridge_content = self._stream_content(data, metadata)

            self._compare_attributi_and_karabo_data()

        # cache data
        for source, source_content in self.attributi.items():
            if source in self.karabo_bridge_content["data"]:
                for key in source_content.keys():
                    if key in self.karabo_bridge_content["data"][source]:

                        self.cache[source][key].append(data[source][key])

        ##
        source = list(k.attributi.keys())[0]
        print("get train {}".format(metadata[source]["timestamp.tid"]))

        return data, metadata

    def run_stats(self):
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

    data, metadata = karabo_data.cache_next_train()
