# import logging
# import pickle
# from pathlib import Path
# from typing import Optional
#
# from tap import Tap
#
# from amarcord.amici.xfel.karabo_bridge_slicer import KaraboBridgeSlicer
# from amarcord.amici.xfel.karabo_configuration import parse_karabo_configuration_file
# from amarcord.amici.xfel.karabo_general import ingest_attributi
# from amarcord.amici.xfel.karabo_general import ingest_karabo_action
# from amarcord.db.constants import ONLINE_SOURCE_NAME
# from amarcord.db.db import DB
# from amarcord.db.dbcontext import CreationMode
# from amarcord.db.dbcontext import DBContext
# from amarcord.db.proposal_id import ProposalId
# from amarcord.db.tables import create_tables
# from amarcord.util import natural_key
#
# PROPOSAL_ID = ProposalId(900188)
#
# logging.basicConfig(
#     format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     level=logging.INFO,
# )
#
# logger = logging.getLogger(__name__)
#
#
# class Arguments(Tap):
#     dump_path: str  # Path with the dumped files from Karabo
#     maximum_files: Optional[int] = None  # Number of maximum files to ingest
#     karabo_config_file: str = "./config.yml"  # Karabo configuration file
#     db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
#
#
# def main() -> None:
#     args = Arguments(underscores_to_dashes=True).parse_args()
#
#     config = parse_karabo_configuration_file(Path(args.karabo_config_file))
#
#     dbcontext = DBContext(args.db_connection_url)
#
#     tables = create_tables(dbcontext)
#
#     if args.db_connection_url.startswith("sqlite://"):
#         dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
#
#     db = DB(dbcontext, tables)
#
#     karabo_data = KaraboBridgeSlicer(config)
#
#     with db.connect() as conn:
#         if args.db_connection_url.startswith("sqlite://") and not db.have_proposals(
#             conn
#         ):
#             db.add_proposal(conn, PROPOSAL_ID)
#
#     ingest_attributi(db, karabo_data.get_attributi())
#
#     files = list(Path(args.dump_path).iterdir())
#
#     if not files:
#         logger.warning("No files in directory %s", args.dump_path)
#         return
#
#     i = 0
#     for fn in sorted(files, key=lambda x: natural_key(x.name)):
#         if args.maximum_files is not None and i >= args.maximum_files:
#             break
#         if i % 1000 == 0:
#             logger.info("still ingesting, current frame %s", fn)
#         i += 1
#
#         with fn.open("rb") as f:
#             data, metadata = pickle.load(f)
#
#             for action in karabo_data.run_definer(data, metadata):
#                 logger.info("%s, Karabo action type %s", fn, type(action))
#                 logger.debug("action data: %s", action)
#
#                 with db.connect() as conn:
#                     ingest_karabo_action(
#                         action, ONLINE_SOURCE_NAME, conn, db, PROPOSAL_ID
#                     )
#
#     logger.info("finished ingesting")
#
#
# if __name__ == "__main__":
#     main()
