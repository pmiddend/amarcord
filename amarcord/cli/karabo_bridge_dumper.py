import logging
import pickle
from typing import Optional

import karabo_bridge
from tap import Tap

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    karabo_client_url: str  # URL of the Karabo client
    events: Optional[int] = None  # Number of events to record at most
    prefix: str  # Filename prefix for the pickled files

    """Dump the stream from the Karabo bridge"""


def main() -> None:
    args = Arguments(underscores_to_dashes=True).parse_args()

    karabo_client = karabo_bridge.Client(args.karabo_client_url)

    logger.info(f"Logging events to {args.prefix}_$i.pickle")
    i = 0
    while True:
        if args.events and i == args.events:
            logger.info("Number of events reached, finishing")
            break
        i += 1

        # pylint: disable=not-callable
        client_data = karabo_client.next()

        with open(f"{args.prefix}_{i}.pickle", "wb") as fh:
            pickle.dump(client_data, fh, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    main()
