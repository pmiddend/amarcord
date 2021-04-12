# type: ignore
# pylint: skip-file
import os
import argparse
import h5py
import karabo_bridge

parser = argparse.ArgumentParser(description="Dump the stream from the Karabo bridge.")
parser.add_argument(
    "karabo-client-URL",
    metavar="Karabo client URL",
    type=str,
    help="URL of the Karabo client",
)
parser.add_argument(
    "--events-to-record",
    metavar="N",
    default=100,
    help="events to record (default: %(default)s)",
)

args = parser.parse_args()

#
karabo_client = karabo_bridge.Client(args.karabo_client)

for i in range(args.events_to_record):
    data, metadata = karabo_client.next()

    # the trainId
    trains = set([source["timestamp.tid"] for source in metadata.values()])

    if len(trains) == 1:
        trainId = list(trains)[0]

    else:
        continue

    with h5py.File("{}.h5".format(trainId), "w") as fh:

        for source, content in metadata.items():
            group = os.path.join("metadata", source)
            fh.create_group(group)

            for key, value in content.items():
                fh.create_dataset(os.path.join(group, key), data=value)

        for source, content in data.items():
            group = os.path.join("data", source)
            fh.create_group(group)

            for key, value in content.items():

                # value is None
                if value is None:
                    fh.create_dataset(os.path.join(group, key), shape=())
                    continue

                # string
                if isinstance(value, str):
                    value = value.encode()

                # list of strings
                if hasattr(value, "__len__"):
                    if all([isinstance(vi, str) for vi in value]):
                        value = [vi.encode() for vi in value]

                try:
                    fh.create_dataset(os.path.join(group, key), data=value)
                except TypeError:
                    print(
                        "Ignoring {}//{} [{}] {}".format(group, key, type(value), value)
                    )
