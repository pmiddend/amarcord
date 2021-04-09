from typing import Any, List, Dict
import os
import yaml


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


def _stream_content(
    data: Dict[str, Any], metadata: Dict[str, Any]
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


# at the first run check there are extra entries in the stream


def _explicitize_attributo(
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
    attributo = locals()

    action_choice = ["average", "check_if_constant"]
    if action not in action_choice:
        raise ValueError(
            "Action must be either '{}'...".format("' or '".join(action_choice))
        )

    return attributo


def _parse_configuration(
    configuration: Dict[str, Any]
) -> Dict[str, List[Dict[str, Any]]]:
    """Parse the configuration file

    Args:
        configuration (Dict[str, Any]): The configuration

    Returns:
        Dict[str, List[Dict[str, Any]]]: A dictionary of attributi
    """
    entry: Dict[str, List[Dict[str, Any]]] = {}

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
                    entry[gi].append(_explicitize_attributo(**attributo))

                except TypeError:
                    raise TypeError(
                        "Wrong attributo definition in {}::{}".format(gi, attributo)
                    )

    return entry


class KaraboBridge:
    def __init__(self, client_endpoint: str, karabo_devices: Dict[str, Any],) -> None:
        """Calculate statistics for devices exposed by the Karabo Bridge

        Args:
            client_endpoint (str): Karabo Bridge endpoint (see karabo_bridge.Client).
            karabo_devices (Dict[str, Any]): Dictionary of devices to be analyzed.
        """

        # Karabo Bridge client
        self.client_endpoint = client_endpoint

        self._client = Client(self.client_endpoint)

        # Karabo devices
        self.karabo_devices = karabo_devices

        # data from the Bridge
        self.cache = {}
        self._initialize_cache()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return

    def _initialize_cache(self) -> None:
        """Initialize arrays holding data
        """

        for ki, vi in self.karabo_devices.items():
            self.cache[ki] = {li: [] for li in vi}

    def next_train(self, verbose=True) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get the next train from the Karabo Bridge

        Args:
            verbose (bool, optional): [description]. Defaults to True.

        Returns:
            Tuple[Dict[str, Any], Dict[str, Any]]: data, metadata
        """

        # get next train
        data, _ = self._client.next()

        if verbose:
            print("Available devices:")

            for ki, vi in data.items():
                print("  {}\n    {}".format(ki, "\n    ".join([i for i in vi.keys()])))

        # cache data
        for ki, vi in self.karabo_devices.items():
            if ki in data.keys():
                for li in vi:
                    if li in data[ki].keys():

                        self.cache[ki][li].append(data[ki][li])
