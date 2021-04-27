import argparse
import logging
import pickle

import karabo_bridge

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dump the stream from the Karabo bridge."
    )
    parser.add_argument(
        "karabo_client_URL",
        metavar="URL",
        help="URL of the Karabo client",
    )
    parser.add_argument(
        "--events", metavar="N", type=int, help="events to record", required=False
    )
    parser.add_argument("--prefix", type=str, help="filename prefix", required=True)

    args = parser.parse_args()

    karabo_client = karabo_bridge.Client(args.karabo_client_URL)

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
