# type: ignore
# pylint: skip-file
import argparse
import os
import pickle
from typing import Any
from typing import Dict

import yaml

from amarcord.amici.karabo_online import KaraboBridgeSlicer
from amarcord.cli.daemon import ingest_attributi
from amarcord.cli.daemon import ingest_karabo_action
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

PROPOSAL_ID = ProposalId(1)

parser = argparse.ArgumentParser(
    description="Read dumps of the stream from the Karabo bridge and run some tests."
)
parser.add_argument(
    "datasets",
    metavar="PICKLE",
    nargs="+",
    help="List of pickle files to be loaded (minimum 10)",
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

    dbcontext = DBContext("sqlite:////tmp/testdatabase.sqlite")
    tables = create_tables(dbcontext)

    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    db = DB(
        dbcontext,
        tables,
    )
    # with db.connect() as conn:
    #    db.add_proposal(conn, PROPOSAL_ID)

    ingest_attributi(db, karabo_data.get_attributi())

    for trainId in trainId_list:
        data, metadata = (
            bridge_content[trainId]["data"],
            bridge_content[trainId]["metadata"],
        )

        for action in karabo_data.run_definer(data, metadata):
            with db.connect() as conn:
                ingest_karabo_action(action, conn, db, PROPOSAL_ID)

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

        for action in karabo_data.run_definer(data, metadata):
            with db.connect() as conn:
                ingest_karabo_action(action, conn, db, PROPOSAL_ID)

    #
    position = 1  # or 0
    position_size = 4

    position2 = 7  # or 0
    position_size2 = 55

    print("RunId is changing...")
    karabo_data = KaraboBridgeSlicer(**config["Karabo_bridge"])

    for trainId in trainId_list:

        data, metadata = (
            bridge_content[trainId]["data"],
            bridge_content[trainId]["metadata"],
        )

        # new run starts
        if trainId >= trainId_list[position] and trainId_list[position] + position_size:
            runId = data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"]

            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"] = runId + 1
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.beginAt.value"
            ] = trainId_list[position]
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.length.value"] = 0

        # new run is over
        if trainId >= trainId_list[position] + position_size:

            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"] = runId + 1
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.beginAt.value"
            ] = trainId_list[position]
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.length.value"
            ] = position_size

            runId = data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"]

        # new run starts
        if (
            trainId >= trainId_list[position2]
            and trainId_list[position2] + position_size2
        ):
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"] = runId + 1
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.beginAt.value"
            ] = trainId_list[position2]
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.length.value"] = 0

        # if trainId == trainId_list[42]:
        #    print(";;;;;;;;;;;;;;;;;;;;")
        #    data["SPB_DA_USR/MDL/AMARCORD_INFO"]["darkRunNumber.value"] = 101

        # new run is over
        if trainId >= trainId_list[position2] + position_size2:

            data["SPB_DAQ_DATA/DM/RUN_CONTROL"]["runDetails.runId.value"] = runId + 1
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.beginAt.value"
            ] = trainId_list[position2]
            data["SPB_DAQ_DATA/DM/RUN_CONTROL"][
                "runDetails.length.value"
            ] = position_size2

        for action in karabo_data.run_definer(data, metadata, averaging_interval=2):
            with db.connect() as conn:
                ingest_karabo_action(action, conn, db, PROPOSAL_ID)
        # print("{}: {}".format(trainId, type(karabo_data.run_definer(data, metadata))))

# remove one device
# compute and test aveage
# avoid to close a run
# run will never start (exceed the train_cache)
