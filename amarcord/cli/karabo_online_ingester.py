import logging

import karabo_bridge

from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer
from amarcord.amici.xfel.karabo_online import load_configuration

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


if __name__ == "__main__":

    config = load_configuration("./config.yml")

    # instantiate the Karabo bridge client
    client_endpoint = config["Karabo_bridge"]["client_endpoint"]
    client = karabo_bridge.Client(client_endpoint)

    logging.info("Connected to the Karabo bridge at {}\n".format(client_endpoint))

    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    while True:
        # pylint: disable=not-callable
        data, metadata = client.next()

        karabo_data.run_definer(data, metadata)
