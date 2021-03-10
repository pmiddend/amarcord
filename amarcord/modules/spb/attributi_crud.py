import datetime
import getpass
import logging
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

from PyQt5.QtCore import QModelIndex, QPoint, QUrl, Qt
from PyQt5.QtGui import QDesktopServices
from PyQt5.QtWidgets import (
    QAbstractItemView,
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
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    AttributiMap,
    attributo_type_to_string,
    pretty_print_attributo,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.db.db import Connection, DB, DBSample
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.new_attributo_dialog import new_attributo_dialog
from amarcord.qt.datetime import print_natural_delta
from amarcord.qt.declarative_table import Column, Data, DeclarativeTable, Row
from amarcord.qt.image_viewer import display_image_viewer
from amarcord.qt.numeric_input_widget import NumericInputValue
from amarcord.qt.validators import (
    Partial,
)

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

        self._attributi_id_edit = QLineEdit()
        right_form_layout.addRow(
            "ID",
            self._attributi_id_edit,
        )
        self._attributi_id_edit.textEdited.connect(self._attributi_id_change)

        self._attributi_description_edit = QLineEdit()
        right_form_layout.addRow(
            "Description",
            self._attributi_description_edit,
        )
        self._attributi_description_edit.textEdited.connect(
            self._attributi_description_change
        )

        self._submit_widget = QWidget()
        self._submit_layout = QHBoxLayout()
        self._submit_layout.setContentsMargins(0, 0, 0, 0)
        self._submit_widget.setLayout(self._submit_layout)
        self._add_button = self._create_add_button()
        self._add_button.setEnabled(True)
        self._submit_layout.addWidget(self._add_button)

        self._attributo_manual_changes: Dict[AttributoId, Any] = {}
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

    def _attributi_id_change(self, new_text: str) -> None:
        pass

    def _attributi_description_change(self, new_text: str) -> None:
        pass

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
        b.clicked.connect(self._add_sample)
        return b

    def _create_edit_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload), "Edit sample"
        )
        b.clicked.connect(self._edit_sample)
        return b

    def _create_cancel_button(self):
        b = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogCancelButton), "Cancel"
        )
        b.clicked.connect(self._cancel_edit)
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

    def _add_sample(self) -> None:
        with self._db.connect() as conn:
            self._db.add_sample(conn, self._current_sample)
            self._reset_input_fields()
            self._log_widget.setText("Sample successfully added!")
            self._fill_table()

    def _edit_sample(self) -> None:
        with self._db.connect() as conn:
            self._db.edit_sample(conn, self._current_sample)
            self._log_widget.setText("Sample successfully edited!")
            self._fill_table()

    def _reset_input_fields(self):
        self._average_crystal_size_edit.set_value(None)
        self._comment_edit.setText("")
        self._creator_edit.setText("")
        self._seed_stock_used_edit.setText("")
        self._plate_origin_edit.setText("")
        self._crystallization_method_edit.setText("")
        self._shaking_strength_edit.set_value(None)
        self._shaking_time_edit.set_value(None)
        self._crystallization_temperature_edit.set_value(None)
        self._crystal_shape_edit.set_value(None)
        self._filters_edit.set_value(None)
        self._compounds_edit.set_value(None)
        self._micrograph_edit.set_value(None)
        self._protocol_edit.set_value(None)
        self._incubation_time_edit.set_value(None)
        self._crystal_settlement_volume_edit.set_value(None)
        self._current_sample = _empty_sample()
        self._protein_concentration_edit.set_value(None)

    def _target_id_change(self, new_id: int) -> None:
        self._current_sample = replace(self._current_sample, target_id=new_id)
        self._reset_button()

    def _comment_edit_change(self, new_comment: str) -> None:
        self._current_sample = replace(self._current_sample, comment=new_comment)
        self._reset_button()

    def _creator_edit_change(self, new_creator: str) -> None:
        self._current_sample = replace(self._current_sample, creator=new_creator)
        self._reset_button()

    def _crystallization_method_edit_change(
        self, new_crystallization_method: str
    ) -> None:
        self._current_sample = replace(
            self._current_sample, crystallization_method=new_crystallization_method
        )
        self._reset_button()

    def _plate_origin_edit_change(self, new_plate_origin: str) -> None:
        self._current_sample = replace(
            self._current_sample, plate_origin=new_plate_origin
        )
        self._reset_button()

    def _seed_stock_used_edit_change(self, new_seed_stock_used: str) -> None:
        self._current_sample = replace(
            self._current_sample, seed_stock_used=new_seed_stock_used
        )
        self._reset_button()

    def _shaking_time_change(self, value: Union[str, datetime.timedelta]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, shaking_time=value)
        self._reset_button()

    def _crystal_shape_change(self, value: Union[str, List[float]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, crystal_shape=value)
        self._reset_button()

    def _filters_change(self, value: Union[str, List[str]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, filters=value)
        self._reset_button()

    def _micrograph_change(self, value: str) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, micrograph=value)
        self._display_micrograph_button.setEnabled(
            self._current_sample.micrograph is not None
        )
        self._reset_button()

    def _protocol_change(self, value: str) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, protocol=value)
        self._open_protocol_button.setEnabled(self._current_sample.protocol is not None)
        self._reset_button()

    def _compounds_change(self, value: Union[str, List[int]]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, compounds=value)
        self._reset_button()

    def _incubation_time_change(self, value: Union[str, datetime.datetime]) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, incubation_time=value)
        self._reset_button()

    def _crystallization_temperature_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(
                self._current_sample, crystallization_temperature=value
            )
        self._reset_button()

    def _protein_concentration_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(
                self._current_sample, protein_concentration=value
            )
        self._reset_button()

    def _crystal_settlement_volume_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(
                self._current_sample, crystal_settlement_volume=value
            )
        self._reset_button()

    def _shaking_strength_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(self._current_sample, shaking_strength=value)
        self._reset_button()

    def _average_crystal_size_change(self, value: NumericInputValue) -> None:
        if not isinstance(value, Partial):
            self._current_sample = replace(
                self._current_sample, average_crystal_size=value
            )
        self._reset_button()

    def _button_enabled(self) -> bool:
        return (
            self._average_crystal_size_edit.valid_value()
            and self._crystal_shape_edit.valid_value()
            and self._incubation_time_edit.valid_value()
            and self._crystallization_temperature_edit.valid_value()
            and self._shaking_time_edit.valid_value()
            and self._shaking_strength_edit.valid_value()
            and self._protein_concentration_edit.valid_value()
            and self._crystal_settlement_volume_edit.valid_value()
            and self._filters_edit.valid_value()
            and self._compounds_edit.valid_value()
        )

    def _reset_button(self) -> None:
        self._add_button.setEnabled(self._button_enabled())

    def _short_name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, short_name=new_name)
        self._reset_button()

    def _name_changed(self, new_name: str) -> None:
        self._current_sample = replace(self._current_sample, name=new_name)
        self._reset_button()

    def _fill_table(self, conn: Optional[Connection] = None) -> None:
        if conn is not None:
            self._samples = self._db.retrieve_samples(conn)
            self._targets = self._db.retrieve_targets(conn)
        else:
            with self._db.connect() as conn_:
                self._samples = self._db.retrieve_samples(conn_)
                self._targets = self._db.retrieve_targets(conn_)

        self._target_id_edit.reset_items(
            [(s.short_name, cast(int, s.id)) for s in self._targets]
        )
        self._sample_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._sample_table.clear()
        attributi_headers = [
            k.description for k in self._attributi_table.metadata.values()
        ]
        headers = [
            "ID",
            "Created",
            "Incubation Time",
            "Crystallization Temperature",
            "Avg Crystal Size",
            "Crystal Shape",
            "Target",
            "Shaking Time",
            "Shaking Strength",
            "Protein Concentration",
            "Crystal Settlement Volume",
            "Seed Stock Used",
            "Plate Origin",
            "Creator",
            "Crystallization Method",
            "Filters",
            "Compounds",
        ] + attributi_headers
        self._sample_table.setColumnCount(len(headers))
        self._sample_table.setHorizontalHeaderLabels(headers)
        self._sample_table.setRowCount(len(self._samples))
        self._sample_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._sample_table.verticalHeader().hide()

        for row, sample in enumerate(self._samples):
            built_in_columns = (
                str(sample.id),
                str(sample.created),
                sample.incubation_time.strftime(DATE_TIME_FORMAT)
                if sample.incubation_time is not None
                else "",
                str(sample.crystallization_temperature)
                if sample.crystallization_temperature is not None
                else "",
                str(sample.average_crystal_size),
                ", ".join(str(s) for s in sample.crystal_shape)
                if sample.crystal_shape is not None
                else "",
                [t.short_name for t in self._targets if sample.target_id == t.id][0],
                print_natural_delta(sample.shaking_time)
                if sample.shaking_time is not None
                else "",
                sample.shaking_strength if sample.shaking_strength is not None else "",
                str(sample.protein_concentration)
                if sample.protein_concentration is not None
                else "",
                sample.comment,
                str(sample.crystal_settlement_volume)
                if sample.crystal_settlement_volume
                else "",
                sample.seed_stock_used,
                sample.plate_origin,
                sample.creator,
                sample.crystallization_method,
                ", ".join(sample.filters) if sample.filters is not None else "",
                ", ".join(sample.compounds if sample is not None else []) if sample.compounds is not None else "",  # type: ignore
            )
            for col, column_value in enumerate(built_in_columns):
                self._sample_table.setItem(row, col, QTableWidgetItem(column_value))  # type: ignore
            i = len(built_in_columns) - 1
            for attributo_id in self._attributi_table.metadata:
                attributo_value = sample.attributi.select(attributo_id)
                self._sample_table.setItem(
                    row,
                    i,
                    QTableWidgetItem(
                        pretty_print_attributo(
                            self._attributi_table.metadata.get(attributo_id, None),
                            attributo_value.value
                            if attributo_value is not None
                            else None,
                        )
                    ),
                )
                i += 1

        self._sample_table.resizeColumnsToContents()
