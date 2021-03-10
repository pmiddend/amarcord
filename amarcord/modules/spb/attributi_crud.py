import datetime
import getpass
import logging
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any, Optional, cast

from PyQt5.QtCore import QModelIndex, QPoint, QUrl, Qt
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtWidgets import (
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMenu,
    QMessageBox,
    QPushButton,
    QSplitter,
    QStyle,
    QVBoxLayout,
    QWidget,
)

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    AttributiMap,
    PropertyInt,
    attributo_type_to_string,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import Connection, DB, DBSample
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.new_attributo_dialog import new_attributo_dialog
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.qt.image_viewer import display_image_viewer

DATE_TIME_FORMAT = "%Y-%m-%d %H:%M"

NEW_SAMPLE_HEADLINE = "New attributo"

logger = logging.getLogger(__name__)


def _empty_sample():
    return DBSample(
        id=None,
        created=datetime.datetime.utcnow(),
        target_id=-1,
        average_crystal_size=None,
        crystal_shape=None,
        incubation_time=None,
        crystallization_temperature=None,
        protein_concentration=None,
        shaking_time=None,
        shaking_strength=None,
        comment="",
        crystal_settlement_volume=None,
        seed_stock_used="",
        plate_origin="",
        creator=getpass.getuser(),
        crystallization_method="",
        filters=None,
        compounds=None,
        micrograph=None,
        protocol=None,
        attributi=AttributiMap({}),
    )


class AttributiCrud(QWidget):
    def __init__(
        self,
        context: Context,
        tables: DBTables,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._db = DB(context.db, tables)

        root_layout = QHBoxLayout()
        self.setLayout(root_layout)

        root_widget = QSplitter(self)
        root_layout.addWidget(root_widget)

        with self._db.connect() as conn:
            self._attributi = [
                TabledAttributo(k, attributo)
                for k, attributi in self._db.retrieve_attributi(conn).items()
                for attributo in attributi.values()
            ]
            self._attributi_table = DeclarativeTable(data=self._create_table_data())

        root_widget.addWidget(self._attributi_table)

        self._current_sample = _empty_sample()
        right_widget = QWidget()
        right_root_layout = QVBoxLayout()
        right_widget.setLayout(right_root_layout)
        self._right_headline = QLabel(NEW_SAMPLE_HEADLINE)
        self._right_headline.setStyleSheet("font-size: 25pt;")
        self._right_headline.setAlignment(Qt.AlignHCenter)
        right_root_layout.addWidget(self._right_headline)
        right_form_layout = QFormLayout()
        right_root_layout.addLayout(right_form_layout)
        self._log_widget = QLabel()
        self._log_widget.setStyleSheet("QLabel { font: italic; color: green; }")
        self._log_widget.setAlignment(Qt.AlignHCenter)
        right_root_layout.addWidget(self._log_widget)
        right_root_layout.addStretch()
        right_widget.setLayout(right_root_layout)
        root_widget.addWidget(right_widget)

        self._attributo_id_edit = QLineEdit()
        right_form_layout.addRow(
            "ID",
            self._attributo_id_edit,
        )
        self._attributo_id_edit.textEdited.connect(self._attributo_id_change)

        self._attributo_description_edit = QLineEdit()
        right_form_layout.addRow(
            "Description",
            self._attributo_description_edit,
        )
        self._attributo_description_edit.textEdited.connect(
            self._attributi_description_change
        )

        self._attributo_suffix_edit = QLineEdit()
        right_form_layout.addRow(
            "Suffix",
            self._attributo_suffix_edit,
        )
        self._attributo_suffix_edit.textEdited.connect(self._attributo_suffix_change)

        self._attributi_table_combo = ComboBox(
            [(t.pretty_id(), t) for t in AssociatedTable], AssociatedTable.RUN
        )
        right_form_layout.addRow(
            "Table",
            self._attributi_table_combo,
        )
        self._attributi_table_combo.item_selected.connect(self._attributo_table_change)

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(False)
        self._submit_layout.addWidget(self._add_button)

        right_form_layout.addWidget(self._submit_widget)

    def _create_table_data(self) -> Data:
        return Data(
            rows=[
                Row(
                    display_roles=[
                        a.attributo.name,
                        a.attributo.description,
                        attributo_type_to_string(a.attributo),
                        a.table.pretty_id(),
                    ],
                    edit_roles=[None, None, None, None],
                    right_click_menu=partial(self._slot_right_click, a),
                )
                for a in self._attributi
            ],
            columns=[
                Column(header_label="ID", editable=False),
                Column(header_label="Description", editable=False),
                Column(header_label="Type", editable=False),
                Column(header_label="Table", editable=False),
            ],
            row_delegates={},
            column_delegates={},
        )

    def _slot_right_click(self, a: TabledAttributo, p: QPoint) -> None:
        menu = QMenu(self)
        deleteAction = menu.addAction(
            self.style().standardIcon(QStyle.SP_DialogCancelButton),
            "Delete attributo",
        )
        action = menu.exec_(p)
        if action == deleteAction:
            result = QMessageBox(  # type: ignore
                QMessageBox.Critical,
                f"Delete attributo “{a.attributo.name}”",
                f"Are you sure you want to delete attributo <b>“{a.attributo.pretty_id()}”</b> from table <b>“{a.table.pretty_id()}”</b>?",
                QMessageBox.Yes | QMessageBox.Cancel,
                self,
            ).exec()

            if result == QMessageBox.Yes:
                with self._db.connect() as conn:
                    self._db.delete_attributo(
                        conn,
                        a.table,
                        a.attributo.name,
                    )
                    self._slot_refresh(conn)

    def _slot_refresh(self, conn: Connection) -> None:
        self._attributi = [
            TabledAttributo(k, attributo)
            for k, attributi in self._db.retrieve_attributi(conn).items()
            for attributo in attributi.values()
        ]
        self._attributi_table.set_data(self._create_table_data())

    def _attributo_id_taken(
        self, table: AssociatedTable, attributo_id: AttributoId
    ) -> bool:
        return any(
            t
            for t in self._attributi
            if t.table == table and t.attributo.name == attributo_id
        )

    def _update_add_button(self) -> None:
        self._add_button.setEnabled(
            bool(self._attributo_id_edit.text())
            and not self._attributo_id_taken(
                cast(AssociatedTable, self._attributi_table_combo.current_value()),
                AttributoId(self._attributo_id_edit.text()),
            )
        )

    def _attributo_id_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributo_suffix_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributi_description_change(self, _new_text: str) -> None:
        self._update_add_button()

    def _attributo_table_change(self, _new_table: AssociatedTable) -> None:
        self._update_add_button()

    def _slot_new_attributo(self) -> None:
        new_column = new_attributo_dialog(self._attributi_table.metadata.keys(), self)

        if new_column is None:
            return

        with self._db.connect() as conn:
            self._db.add_attributo(
                conn,
                name=new_column.name,
                description=new_column.description,
                suffix=None,
                prop_type=new_column.rich_property_type,
                associated_table=AssociatedTable.RUN,
            )
            self._slot_refresh(conn)

    def _attributo_change(self, attributo: AttributoId, value: Any) -> None:
        with self._db.connect() as conn:
            logger.info("Setting attributo %s to %s", attributo, value)
            self._current_sample.attributi.set_single_manual(attributo, value)
            # We could immediately change the attribute, but we have this "Save changes" button
            # if self._current_sample.id is not None:
            #     self._db.update_sample_attributo(
            #         conn, self._current_sample.id, attributo, value
            #     )
            self._slot_refresh(conn)

    def _display_micrograph(self) -> None:
        assert self._current_sample.micrograph is not None
        display_image_viewer(Path(self._current_sample.micrograph), parent=self)

    def _open_protocol(self) -> None:
        assert self._current_sample.protocol is not None
        QDesktopServices.openUrl(QUrl(f"file://{self._current_sample.protocol}"))

    def _choose_protocol(self) -> None:
        result, _ = QFileDialog.getOpenFileName(
            self,
            "Choose a protocol file",
            str(self._proposal_file_path),
        )
        if not result:
            return

        self._protocol_edit.setText(result)
        self._current_sample = replace(self._current_sample, protocol=result)
        self._open_protocol_button.setEnabled(True)

    def _choose_micrograph(self) -> None:
        result, _ = QFileDialog.getOpenFileName(
            self,
            "Choose an image file",
            str(self._proposal_file_path),
        )
        if not result:
            return

        self._micrograph_edit.setText(result)
        self._current_sample = replace(self._current_sample, micrograph=result)
        self._display_micrograph_button.setEnabled(True)

    def _delete_sample(self) -> None:
        row_idx = self._sample_table.currentRow()
        sample = self._samples[row_idx]

        assert sample.id is not None

        result = QMessageBox(  # type: ignore
            QMessageBox.Critical,
            f"Delete “{sample.id}”",
            f"Are you sure you want to delete sample “{sample.id}”?",
            QMessageBox.Yes | QMessageBox.Cancel,
            self,
        ).exec()

        if result == QMessageBox.Yes:
            with self._db.connect() as conn:
                self._db.delete_sample(conn, sample.id)
                self._log_widget.setText(f"Sample “{sample.id}” deleted!")
                self._fill_table()
                if self._current_sample.id == sample.id:
                    self._reset_input_fields()
                    self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _create_add_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogOkButton), "Add sample"
        )
        b.clicked.connect(self._add_attributo)
        return b

    def _slot_row_selected(self, index: QModelIndex) -> None:
        self._attributo_manual_changes.clear()
        self._current_sample = self._samples[index.row()]
        self._right_headline.setText(f"Edit sample “{self._current_sample.id}”")
        self._comment_edit.setText(self._current_sample.comment)
        self._seed_stock_used_edit.setText(self._current_sample.seed_stock_used)
        self._plate_origin_edit.setText(self._current_sample.plate_origin)
        self._target_id_edit.set_current_value(self._current_sample.target_id)
        self._creator_edit.setText(self._current_sample.creator)
        self._crystallization_method_edit.setText(
            self._current_sample.crystallization_method
        )
        self._average_crystal_size_edit.set_value(
            self._current_sample.average_crystal_size
        )
        self._crystal_settlement_volume_edit.set_value(
            self._current_sample.crystal_settlement_volume
        )
        self._crystallization_temperature_edit.set_value(
            self._current_sample.crystallization_temperature
        )
        self._shaking_time_edit.set_value(
            self._current_sample.shaking_time  # type: ignore
            if self._current_sample.shaking_time is not None
            else None
        )
        self._micrograph_edit.set_value(self._current_sample.micrograph)  # type: ignore
        self._protocol_edit.set_value(self._current_sample.protocol)  # type: ignore
        self._shaking_strength_edit.set_value(self._current_sample.shaking_strength)
        # noinspection PyTypeChecker
        self._crystal_shape_edit.set_value(self._current_sample.crystal_shape)  # type: ignore
        # noinspection PyTypeChecker
        self._filters_edit.set_value(self._current_sample.filters)  # type: ignore
        # noinspection PyTypeChecker
        self._compounds_edit.set_value(self._current_sample.compounds)  # type: ignore
        self._incubation_time_edit.setText(
            self._current_sample.incubation_time.strftime(DATE_TIME_FORMAT)
            if self._current_sample.incubation_time is not None
            else ""
        )
        self._clear_submit()
        self._submit_layout.addWidget(self._create_edit_button())
        self._submit_layout.addWidget(self._create_cancel_button())

        self._attributi_table.data_changed(
            self._current_sample.attributi,
            self._attributi_table.metadata,
            sample_ids=[],
        )

    def _clear_submit(self):
        while True:
            result = self._submit_layout.takeAt(0)
            if result is not None and result.widget() is not None:
                result.widget().hide()
            if result is None:
                break

    def _cancel_edit(self) -> None:
        self._attributo_manual_changes.clear()
        self._clear_submit()
        self._submit_layout.addWidget(self._create_add_button())
        self._reset_input_fields()
        self._right_headline.setText(NEW_SAMPLE_HEADLINE)

    def _add_attributo(self) -> None:
        with self._db.connect() as conn:
            self._db.add_attributo(
                conn,
                self._attributo_id_edit.text(),
                self._attributo_description_edit.text(),
                cast(AssociatedTable, self._attributi_table_combo.current_value()),
                self._attributo_suffix_edit.text(),
                PropertyInt(),
            )
            self._reset_input_fields()
            self._slot_refresh(conn)
            self._log_widget.setText("Attributo added!")

    def _reset_input_fields(self) -> None:
        self._attributo_id_edit.setText("")
        self._attributo_description_edit.setText("")
        self._attributo_suffix_edit.setText("")
        self._update_add_button()
