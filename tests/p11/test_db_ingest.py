# def test_db_ingest_diffractions(db) -> None:
#     dbcontext = DBContext("sqlite://", echo=False)
#     diffs = table_diffractions(dbcontext.metadata)
#     crystals = table_crystals(dbcontext.metadata)
#     pucks = table_pucks(dbcontext.metadata)
#     dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)
#
#     with dbcontext.connect() as conn:
#         crystal_id = "c1"
#         conn.execute(sa.insert(pucks).values(puck_id="p1", puck_type=PuckType.UNI))
#         conn.execute(
#             sa.insert(crystals).values(
#                 crystal_id=crystal_id, puck_id="p1", puck_position_id=1
#             )
#         )
#
#         target_list = [
#             P11Target(
#                 "t1",
#                 pucks=[
#                     P11Puck(
#                         "p1",
#                         1,
#                         runs=[
#                             P11Run(
#                                 1,
#                                 parse_p11_info_file(
#                                     Path(__file__).parent / "info.txt", UnitRegistry()
#                                 ),
#                                 data_raw_filename_pattern=None,
#                                 microscope_image_filename_pattern=None,
#                                 processed_path=None,
#                             )
#                         ],
#                     )
#                 ],
#             )
#         ]
#         has_warnings = ingest_diffractions_for_targets(
#             conn,
#             diffs,
#             crystals,
#             target_list,
#         )
#         assert not has_warnings
#
#     with dbcontext.connect() as conn:
#         diffraction_ids = conn.execute(sa.select([diffs.c.crystal_id])).fetchall()
#
#         assert len(diffraction_ids) == 1
#         assert diffraction_ids[0][0] == crystal_id
#
#
# def test_db_ingest_diffractions_idempotent(db) -> None:
#     dbcontext = DBContext("sqlite://", echo=False)
#     diffs = table_diffractions(dbcontext.metadata)
#     crystals = table_crystals(dbcontext.metadata)
#     pucks = table_pucks(dbcontext.metadata)
#     dbcontext.create_all(creation_mode=CreationMode.DONT_CHECK)
#
#     with dbcontext.connect() as conn:
#         crystal_id = "c1"
#         conn.execute(sa.insert(pucks).values(puck_id="p1", puck_type=PuckType.UNI))
#         conn.execute(
#             sa.insert(crystals).values(
#                 crystal_id=crystal_id, puck_id="p1", puck_position_id=1
#             )
#         )
#
#         target_list = [
#             P11Target(
#                 "t1",
#                 pucks=[
#                     P11Puck(
#                         "p1",
#                         1,
#                         runs=[
#                             P11Run(
#                                 1,
#                                 parse_p11_info_file(
#                                     Path(__file__).parent / "info.txt", UnitRegistry()
#                                 ),
#                                 data_raw_filename_pattern=None,
#                                 microscope_image_filename_pattern=None,
#                                 processed_path=None,
#                             )
#                         ],
#                     )
#                 ],
#             )
#         ]
#         has_warnings = ingest_diffractions_for_targets(
#             conn,
#             diffs,
#             crystals,
#             target_list,
#         )
#         assert not has_warnings
#         has_warnings = ingest_diffractions_for_targets(
#             conn,
#             diffs,
#             crystals,
#             target_list,
#         )
#         assert not has_warnings
#
#     with dbcontext.connect() as conn:
#         diffraction_ids = conn.execute(sa.select([diffs.c.crystal_id])).fetchall()
#
#         assert len(diffraction_ids) == 1
#         assert diffraction_ids[0][0] == crystal_id
