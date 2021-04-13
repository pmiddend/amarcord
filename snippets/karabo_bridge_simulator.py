# type: ignore
# pylint: skip-file
import os
import argparse
import pickle
import yaml
from typing import Dict
from typing import Any
from amarcord.amici.karabo_online import KaraboBridgeSlicer

parser = argparse.ArgumentParser(
    description="Read dumps of the stream from the Karabo bridge and run some tests."
)
parser.add_argument(
    "datasets",
    metavar="PICKLE",
    nargs="+",
    help="List of pickle files to be loaded (minimum 5)",
)
parser.add_argument(
    "--sending-interval",
    metavar="TIME",
    type=float,
    help="sending time (default: %(default)s)",
    default=0.1,
)

args = parser.parse_args()

# helper
def extract_trainId(metadata):
    trainId = set([value["timestamp.tid"] for value in metadata.values()])

    if len(trainId) == 1:
        return list(trainId)[0]
    else:
        raise ValueError("Multiple trainId values")


# read data sets
bridge_content = {}

for di in args.datasets:

    with open(di, "rb") as handle:
        print("\rReading {}     ".format(di), end="")

        dataset_content = pickle.load(handle)
        trainId = extract_trainId(dataset_content["metadata"])

        bridge_content[trainId] = dataset_content

trainId_list = sorted(list(bridge_content.keys()))

## start the simulation
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

    #
    print("The complete data set...")
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    for trainId in trainId_list:
        data, metadata = (
            bridge_content[trainId]["data"],
            bridge_content[trainId]["metadata"],
        )

        print(karabo_data.run_definer(data, metadata))

    #
    trainId_at_position = trainId_list[4]

    print("TrainId {} is missing...".format(trainId_at_position))
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    for trainId in trainId_list:

        data, metadata = (
            bridge_content[trainId]["data"],
            bridge_content[trainId]["metadata"],
        )

        if trainId == trainId_at_position:
            continue

        print("{}: {}".format(trainId, karabo_data.run_definer(data, metadata)))

    #
    position = 3
    trainId_at_position = trainId_list[position]

    print("RunId is changing...")
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    for trainId in trainId_list:

        data, metadata = (
            bridge_content[trainId]["data"],
            bridge_content[trainId]["metadata"],
        )

        if trainId == trainId_list[position]:
            runId = data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"]

        elif trainId == trainId_list[position + 1]:
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"] = runId + 1
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.beginAt.value"] = trainId
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.length.value"] = 0

        print("{}: {}".format(trainId, karabo_data.run_definer(data, metadata)))
