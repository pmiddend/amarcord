# type: ignore
# pylint: skip-file
import os
import argparse
import pickle
import karabo_bridge

parser = argparse.ArgumentParser(description="Dump the stream from the Karabo bridge.")
parser.add_argument(
    "karabo_client_URL",
    metavar="URL",
    help="URL of the Karabo client",
)
parser.add_argument(
    "--events",
    metavar="N",
    type=int,
    help="events to record (default: %(default)s)",
    default=100,
)

args = parser.parse_args()

#
karabo_client = karabo_bridge.Client(args.karabo_client_URL)

for i in range(args.events_to_record):
    data, metadata = karabo_client.next()

    # the trainId
    trains = set([source["timestamp.tid"] for source in metadata.values()])

    if len(trains) == 1:
        trainId = list(trains)[0]

    else:
        continue

    with open("{}.pickle".format(trainId), "wb") as fh:
        pickle.dump(
            {"data": data, "metadata": metadata},
            fh,
            protocol=pickle.HIGHEST_PROTOCOL,
        )
