import logging

logger = logging.getLogger(__name__)


# class CrystFELProjectFiles(QWidget):
#     def __init__(
#         self, context: Context, tables: DBTables, proposal_id: ProposalId
#     ) -> None:
#         super().__init__()
#
#         self._proposal_id = proposal_id
#         self._context = context
#         self._db = DB(context.db, tables)
#
#         self._root_layout = QtWidgets.QVBoxLayout(self)
#
#         self._top_bar_layout = QtWidgets.QHBoxLayout()
#         self._make_current_button = QtWidgets.QPushButton(
#             self.style().standardIcon(QtWidgets.QStyle.SP_FileDialogNewFolder),
#             "Make current",
#         )
#         self._make_current_button.clicked.connect(self._slot_make_current)
#         self._refresh_button = QtWidgets.QPushButton(
#             self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload),
#             "Refresh",
#         )
#         self._refresh_button.clicked.connect(self._slot_refresh)
#         self._top_bar_layout.addWidget(self._make_current_button)
#         self._top_bar_layout.addWidget(self._refresh_button)
#         self._top_bar_layout.addStretch()
#         self._root_layout.addLayout(self._top_bar_layout)
#
#         self._bottom_part_layout = QtWidgets.QHBoxLayout()
#         self._root_layout.addLayout(self._bottom_part_layout)
#
#         self._data = self._retrieve_data()
#         self._table = DeclarativeTable(self._data)
#         self._bottom_part_layout.addWidget(self._table)
#
#         self._indexing_param_content = QtWidgets.QGroupBox("Contents:")
#         self._indexing_param_content_layout = QtWidgets.QVBoxLayout()
#         self._indexing_param_content.setLayout(self._indexing_param_content_layout)
#
#         scroll_area = QtWidgets.QScrollArea()
#         self._indexing_param_content_label = QLabel("content here")
#         scroll_area.setWidget(self._indexing_param_content_label)
#         self._indexing_param_content_layout.addWidget(scroll_area)
#         self._indexing_param_content_layout.addStretch()
#
#         self._bottom_part_layout.addWidget(self._indexing_param_content)
#
#     def _slot_refresh(self) -> None:
#         self._data = self._retrieve_data()
#         self._table.set_data(self._data)
#
#     def _slot_make_current(self) -> None:
#         with self._db.connect() as conn:
#             selected_rows = self._table.selectionModel().selectedRows()
#             if not selected_rows:
#                 return
#             selected_row = selected_rows[0].row()
#             self._db.set_latest_indexing_parameter_id(
#                 conn, int(self._data.rows[selected_row].display_roles[0])
#             )
#
#         self._slot_refresh()
#
#     def _row_selected(self, indexing_parameter_id: int) -> None:
#         with self._db.connect() as conn:
#             content = self._db.retrieve_indexing_parameter(
#                 conn, indexing_parameter_id
#             ).project_file_content
#             self._indexing_param_content_label.setText(
#                 "" if content is None else content
#             )
#
#     def _retrieve_data(self) -> Data:
#         with self._db.connect() as conn:
#             now = datetime.datetime.utcnow()
#             return Data(
#                 rows=[
#                     Row(
#                         display_roles=[
#                             str(x.indexing_parameter.id),
#                             humanize.naturaltime(
#                                 now - x.indexing_parameter.project_file_first_discovery
#                             ),
#                             humanize.naturaltime(
#                                 now - x.indexing_parameter.project_file_last_discovery
#                             ),
#                             str(x.number_of_jobs),
#                         ],
#                         edit_roles=[],
#                         double_click_callback=partial(
#                             self._row_selected, x.indexing_parameter.id
#                         ),
#                     )
#                     for x in self._db.retrieve_indexing_parameters(conn)
#                 ],
#                 columns=[
#                     Column(header_label="ID", editable=False),
#                     Column(header_label="First seen", editable=False),
#                     Column(header_label="Last seen", editable=False),
#                     Column(header_label="Jobs", editable=False),
#                 ],
#                 row_delegates={},
#                 column_delegates={},
#             )
