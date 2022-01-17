# import datetime
# import logging
# from typing import Iterable
# from typing import List
# from typing import Optional
# from typing import Tuple
#
# import numpy as np
#
# from amarcord.amici.xfel.karabo_action import KaraboAction
# from amarcord.amici.xfel.karabo_attributi import KaraboAttributi
# from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
# from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
# from amarcord.amici.xfel.karabo_image import KaraboImage
# from amarcord.db.associated_table import AssociatedTable
# from amarcord.db.attributo_id import AttributoId
# from amarcord.db.attributo_type import AttributoType
# from amarcord.db.attributo_type import AttributoTypeDateTime
# from amarcord.db.attributo_type import AttributoTypeDouble
# from amarcord.db.attributo_type import AttributoTypeInt
# from amarcord.db.attributo_type import AttributoTypeList
# from amarcord.db.attributo_type import AttributoTypeString
# from amarcord.db.db import Connection
# from amarcord.db.db import DB
# from amarcord.db.db import RunNotFound
# from amarcord.db.proposal_id import ProposalId
# from amarcord.db.raw_attributi_map import RawAttributiMap
# from amarcord.util import find_by
#
# logger = logging.getLogger(__name__)
#
#
# def karabo_attributi_to_attributi_map(
#     source: str,
#     attributi: RawAttributiMap,
#     karabo_attributi: KaraboAttributi,
#     existing_db_attributi: Iterable[AttributoId],
# ) -> Tuple[RawAttributiMap, List[KaraboImage]]:
#     new_attributi = attributi.copy()
#     images: List[KaraboImage] = []
#     for _group, group_items in karabo_attributi.items():
#         for a in group_items.values():
#             if AttributoId(a.identifier) not in existing_db_attributi:
#                 continue
#             if not a.store:
#                 continue
#             if a.type_ == "image":
#                 images.append(KaraboImage(a.value, a))
#                 continue
#             value = a.value
#             if value is not None:
#                 if isinstance(value, np.integer):
#                     value = int(value)
#                 elif isinstance(value, np.str_):
#                     value = str(value)
#                 elif isinstance(value, np.bool_):
#                     value = int(value)
#                 elif isinstance(value, np.floating):
#                     value = float(value)
#                 elif isinstance(value, np.ndarray):
#                     if np.issubdtype(value.dtype, np.integer):  # type: ignore
#                         value = [int(f) for f in value]
#                     elif np.issubdtype(value.dtype, np.floating):  # type: ignore
#                         value = [float(f) for f in value]
#                     elif np.issubdtype(value.dtype, np.string_):  # type: ignore
#                         value = [str(f) for f in value]
#                     else:
#                         logger.debug(
#                             f"invalid numpy array type {value.dtype} in attributo {a.identifier}"
#                         )
#                         continue
#                 if isinstance(a.value, datetime.datetime):
#                     new_attributi.extend(
#                         source, {AttributoId(a.identifier): value.isoformat()}
#                     )
#                 else:
#                     new_attributi.extend(source, {AttributoId(a.identifier): value})
#     return new_attributi, images
#
#
# def ingest_karabo_action(
#     action: KaraboAction, source: str, conn: Connection, db: DB, proposal_id: ProposalId
# ) -> None:
#     if action.proposal_id != proposal_id:
#         logger.info(
#             "Skipping run %s, proposal ID is %s, expected %s",
#             action.run_id,
#             action.proposal_id,
#             proposal_id,
#         )
#         return
#     attributi = db.retrieve_table_attributi(conn, AssociatedTable.RUN, inherent=False)
#     try:
#         run_attributi = db.retrieve_run(conn, proposal_id, action.run_id).attributi
#         new_attributi, _images = karabo_attributi_to_attributi_map(
#             source, run_attributi, action.attributi, attributi.keys()
#         )
#         db.update_run_attributi(conn, action.run_id, new_attributi)
#     except RunNotFound:
#         run_attributi, _images = karabo_attributi_to_attributi_map(
#             source, RawAttributiMap({}), action.attributi, attributi.keys()
#         )
#         db.add_run(
#             conn,
#             proposal_id,
#             action.run_id,
#             None,
#             run_attributi,
#         )
#
#
# def _unit_to_type(type_str: str, unit_str: Optional[str]) -> AttributoType:
#     if type_str == "str":
#         return AttributoTypeString()
#     if type_str == "datetime":
#         return AttributoTypeDateTime()
#     if type_str == "int":
#         return AttributoTypeInt()
#     if type_str == "decimal":
#         return AttributoTypeDouble(suffix=unit_str)
#     if type_str == "unit_type":
#         return AttributoTypeDouble(suffix=unit_str, standard_unit=True)
#     if type_str == "list[str]":
#         return AttributoTypeList(
#             AttributoTypeString(), min_length=None, max_length=None
#         )
#     if type_str.startswith("list["):
#         list_type = type_str[5:-1]
#         return _unit_to_type(list_type, unit_str)
#         # FIXME: WTF
#         # return AttributoTypeList(
#         #     sub_type=_unit_to_type(list_type, unit_str),
#         #     max_length=None,
#         #     min_length=None,
#         # )
#     raise Exception(f"invalid attributo type {type_str} (unit {unit_str})")
#
#
# def _join_list(l: Iterable[str]) -> str:
#     return "\n".join(f"- {i}" for i in l)
#
#
# def ingest_attributi(db: DB, attributi: KaraboExpectedAttributi) -> None:
#     with db.connect() as conn:
#         run_attributi = db.retrieve_table_attributi(
#             conn, AssociatedTable.RUN, inherent=True
#         )
#
#         # Peel off the source, key and group layers, leaving the attributi
#         unsourced_attributi: List[KaraboAttributo] = [
#             r["attributo"]
#             for k in attributi.values()
#             for r in k.values()
#             if r["attributo"].store
#         ]
#
#         attributi_ids = [k.identifier for k in unsourced_attributi]
#
#         old_attributi = run_attributi.keys()
#
#         new_attributi_ids = attributi_ids - old_attributi
#         not_present_attributi_ids = old_attributi - attributi_ids
#
#         logger.info(
#             "Will create the following Karabo attributi in the database:\n\n%s",
#             _join_list(new_attributi_ids),
#         )
#         logger.info(
#             "The following attributi are stored for a run, but do not come from Karabo (anymore?):\n\n%s",
#             _join_list(not_present_attributi_ids),
#         )
#
#         for n in new_attributi_ids:
#             # pylint: disable=cell-var-from-loop
#             a = find_by(unsourced_attributi, lambda x: x.identifier == n)
#             assert a is not None
#
#             type_ = _unit_to_type(a.type_, a.unit)
#             db.add_attributo(
#                 conn,
#                 n,
#                 a.description,
#                 AssociatedTable.RUN,
#                 type_,
#             )
#
#
import datetime


def parse_weird_karabo_datetime(s: str) -> datetime.datetime:
    return datetime.datetime.strptime(s, "%Y%m%dT%H%M%S.%fZ")
