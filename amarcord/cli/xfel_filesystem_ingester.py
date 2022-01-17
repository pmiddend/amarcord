# import logging
# from pathlib import Path
# from typing import Optional
#
# import numpy as np
# from tap import Tap
#
# from amarcord.amici.xfel.karabo_action import KaraboRunStart
# from amarcord.amici.xfel.karabo_configuration import parse_karabo_configuration_file
# from amarcord.amici.xfel.karabo_general import ingest_attributi
# from amarcord.amici.xfel.karabo_general import ingest_karabo_action
# from amarcord.amici.xfel.xfel_filesystem import FileSystem2Attributo
# from amarcord.db.constants import OFFLINE_SOURCE_NAME
# from amarcord.db.db import DB
# from amarcord.db.proposal_id import ProposalId
# from amarcord.db.tables import create_tables
# from amarcord.db.dbcontext import CreationMode
# from amarcord.db.dbcontext import DBContext
#
# logging.basicConfig(
#     format="%(asctime)s.%(msecs)03d %(levelname)8s [%(module)s] %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     level=logging.DEBUG,
# )
#
#
# class Arguments(Tap):
#     proposal_id: int  # ID of the proposal to ingest data for
#     db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
#     run_id: int  # Run ID to ingest for
#     karabo_config_file: str = "./config.yml"  # Karabo configuration file
#     number_of_bunches: int = 0  # Number of bunches, of not available
#
#
# if __name__ == "__main__":
#     args = Arguments(underscores_to_dashes=True).parse_args()
#
#     config = parse_karabo_configuration_file(Path(args.karabo_config_file))
#     data = FileSystem2Attributo(args.proposal_id, args.run_id, config)
#     data.extract_data(args.number_of_bunches)
#     attributi_definition = data.compute_statistics()
#
#     logging.info("\n\nReduced values:")
#     for group in data.attributi:
#         for attributo in data.attributi[group].values():
#             if attributo.value is None or attributo.source not in data.cache:
#                 continue
#
#             cached_value = data.cache[attributo.source][attributo.key]
#             if not isinstance(cached_value, np.ndarray):
#                 continue
#
#             original_shape: Optional[np.ndarray]
#             try:
#                 original_shape = cached_value.shape  # type: ignore
#             except:
#                 original_shape = None
#
#             logging.info(
#                 "%s (original shape %s): %s %s [%s]",
#                 attributo.identifier,
#                 original_shape,
#                 attributo.value,
#                 attributo.unit if attributo.unit is not None else "",
#                 attributo.type_,
#             )
#
#     if args.db_connection_url:
#         dbcontext = DBContext(args.db_connection_url, echo=True)
#
#         tables = create_tables(dbcontext)
#
#         if args.db_connection_url.startswith("sqlite://"):
#             dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
#
#         db = DB(dbcontext, tables)
#
#         with db.connect() as conn:
#             if args.db_connection_url.startswith("sqlite://"):
#                 if not db.have_proposals(conn):
#                     db.add_proposal(conn, ProposalId(args.proposal_id))
#
#             ingest_attributi(db, data.expected_attributi)
#
#             ingest_karabo_action(
#                 KaraboRunStart(args.run_id, args.proposal_id, data.attributi),
#                 OFFLINE_SOURCE_NAME,
#                 conn,
#                 db,
#                 ProposalId(args.proposal_id),
#             )
