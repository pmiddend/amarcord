from typing import Optional

from PyQt5.QtCore import Qt
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QLabel
from PyQt5.QtWidgets import QLineEdit
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import OperationalError

from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.factories import retrieve_proposal_ids
from amarcord.modules.uicontext import UIContext


class ConnectionDialog(QWidget):
    accepted = pyqtSignal()
    rejected = pyqtSignal()

    def __init__(self, admin_mode: bool, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        self._admin_mode = admin_mode
        self._root_layout = QVBoxLayout(self)

        headline = QLabel("Choose database")
        headline.setStyleSheet("font-size: 25pt;")
        headline.setAlignment(Qt.AlignHCenter)
        self._root_layout.addWidget(headline)

        description = QLabel(
            "<p>Enter the <i>database connection URL</i> here. It should look something like "
            "this:</p><pre>mysql+pymysql://username:password@cfeld-vm05/xfel-foobar</pre><p>You should have gotten "
            "this URL from the PI, from Confluence or from Luca and Philipp.</p>"
        )
        description.setTextFormat(Qt.RichText)

        self._root_layout.addWidget(description)

        input_line_layout = QHBoxLayout()

        input_line_layout.addWidget(QLabel("URL:"))
        self._url_edit = QLineEdit()
        input_line_layout.addWidget(self._url_edit)
        self._url_test_button = QPushButton(
            self.style().standardIcon(QStyle.SP_DialogApplyButton), "Test connection"
        )
        input_line_layout.addWidget(self._url_test_button)
        self._url_test_button.clicked.connect(self._slot_test)

        self._alembic_button = QPushButton(
            self.style().standardIcon(QStyle.SP_MessageBoxCritical), "Call Alembic"
        )
        input_line_layout.addWidget(self._alembic_button)
        self._alembic_button.clicked.connect(self._slot_alembic)

        self._root_layout.addLayout(input_line_layout)

        self._test_result = QLabel("")
        self._root_layout.addWidget(self._test_result)

        self._button_box = QDialogButtonBox(  # type: ignore
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel
        )
        self._button_box.rejected.connect(self.rejected.emit)
        self._button_box.accepted.connect(self.accepted.emit)
        self._button_box.button(QDialogButtonBox.Ok).setEnabled(False)
        self._root_layout.addWidget(self._button_box)

    def _set_error(self, e: str, long_message: Optional[str] = None) -> None:
        if long_message is not None:
            self._test_result.setTextFormat(Qt.RichText)
            self._test_result.setTextInteractionFlags(Qt.TextSelectableByMouse)
            self._test_result.setText(f"<p>{e}</p><pre>{long_message}</pre>")
        else:
            self._test_result.setText(e)
        self._test_result.setStyleSheet("QLabel { color: red; }")

    def _set_success(self, e: str) -> None:
        self._test_result.setText(e)
        self._test_result.setStyleSheet("QLabel { color: green; }")
        self._button_box.button(QDialogButtonBox.Ok).setEnabled(True)

    def _slot_alembic(self) -> None:
        from alembic.config import Config
        from alembic import command

        alembic_cfg = Config()
        alembic_cfg.set_main_option("sqlalchemy.url", self._url_edit.text())
        alembic_cfg.set_main_option("script_location", "alembic")
        # alembic_cfg.attributes["script_location"] = "alembic"
        command.upgrade(alembic_cfg, "head")

    def _slot_test(self) -> None:
        try:
            context = DBContext(self._url_edit.text())
        except ArgumentError:
            self._set_error("The connection URL doesn't seem to be valid!")
            return

        tables = create_tables(context)

        with context.connect():
            try:
                propos = retrieve_proposal_ids(context, tables)
            except OperationalError as e:
                self._set_error(
                    "Connection worked, but the database doesn't look valid!",
                    long_message=str(e),
                )
                return

            if not propos:
                self._set_error("Connecting worked, but we have no proposals!")
            else:
                self._set_success(
                    f"Connecting worked, and we have {len(propos)} proposal(s)!"
                )

    def valid_url(self) -> Optional[str]:
        return (
            self._url_edit.text()
            if self._button_box.button(QDialogButtonBox.Ok).isEnabled()
            else None
        )


def show_connection_dialog(admin_mode: bool, _ui_context: UIContext) -> Optional[str]:
    dialog = QDialog()
    dialog_layout = QHBoxLayout(dialog)
    connection_dialog = ConnectionDialog(admin_mode)
    connection_dialog.rejected.connect(dialog.reject)
    connection_dialog.accepted.connect(dialog.accept)
    dialog_layout.addWidget(connection_dialog)
    result = dialog.exec_()
    return connection_dialog.valid_url() if result == QDialog.Accepted else None
