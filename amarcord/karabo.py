from typing import Any, Dict
import os
import yaml


def load_configuration(descriptor: str):
    # should AMARCORD send the configuration to the daemon?

    if os.path.exists(descriptor):
        with open(descriptor) as fh:
            configuration = yaml.load(fh, Loader=yaml.Loader)

    else:
        raise FileNotFoundError("{} not found...".format(descriptor))

    return configuration


def parse_configuration(stream: Dict[str, Any]):
    # remember to apply default values

    # run information
    if "run" in stream:
        pass

    else:
        raise ValueError("Run configuration missing")

    return


def navigate(stream: Dict[str, Any]):
    """Navigate the stream from the Karabo bridge

    Args:
        stream (Dict[str, Any]): [description]
    """

    for ki, vi in stream.items():
        print("{}".format(ki))

        for kj, vj in vi.items():
            print("  {} {}".format(kj, vj))


# at the first run check there are extra entries in the stream
