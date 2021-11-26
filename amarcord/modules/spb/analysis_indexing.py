import logging

from PyQt5 import QtWidgets
from PyQt5.QtWidgets import QTreeWidget
from PyQt5.QtWidgets import QTreeWidgetItem
from PyQt5.QtWidgets import QWidget

from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context

logger = logging.getLogger(__name__)


def _sample_based_tree(tree: QTreeWidget) -> None:
    first_sample = QTreeWidgetItem(["sample 1 (1 run, 1 indexed)", "", "", ""])
    first_sample.addChild(QTreeWidgetItem(["fom here", "1", "3675", "active"]))
    tree.addTopLevelItem(first_sample)

    second_sample = QTreeWidgetItem(["sample 2 (2 runs, 2 indexed)", "", "", ""])
    second_sample.addChild(QTreeWidgetItem(["fom here", "2", "3676", "active"]))
    second_sample.addChild(QTreeWidgetItem(["fom here", "2", "3677", "blocked"]))
    second_sample.addChild(QTreeWidgetItem(["fom here", "3", "3678", "active"]))
    tree.addTopLevelItem(second_sample)


def _run_based_tree(tree: QTreeWidget) -> None:
    first_run = QTreeWidgetItem(["run 1 (1 indexed)", "", "", ""])
    first_run.addChild(QTreeWidgetItem(["fom here", "1", "3675", "active"]))
    tree.addTopLevelItem(first_run)

    second_run = QTreeWidgetItem(["run 2 (2 indexed)", "", "", ""])
    second_run.addChild(QTreeWidgetItem(["fom here", "2", "3676", "active"]))
    second_run.addChild(QTreeWidgetItem(["fom here", "2", "3677", "blocked"]))
    tree.addTopLevelItem(second_run)

    third_run = QTreeWidgetItem(["run 3 (1 indexed)", "", "", ""])
    third_run.addChild(QTreeWidgetItem(["fom here", "3", "3678", "active"]))
    tree.addTopLevelItem(third_run)


class AnalysisIndexingResults(QWidget):
    def __init__(
        self, context: Context, tables: DBTables, proposal_id: ProposalId
    ) -> None:
        super().__init__()

        self._proposal_id = proposal_id
        self._context = context
        self._db = DB(context.db, tables)

        self._root_layout = QtWidgets.QVBoxLayout(self)

        self._top_bar_layout = QtWidgets.QHBoxLayout()
        self._label_for_group_by = QtWidgets.QLabel("Group by")
        self._group_by = QtWidgets.QComboBox()
        self._group_by.addItem("Sample")
        self._group_by.addItem("Run ID")
        self._group_by.currentTextChanged.connect(self._slot_group_changed)
        self._label_for_filter = QtWidgets.QLabel("Filter")
        self._filter = QtWidgets.QLineEdit()
        self._refresh_button = QtWidgets.QPushButton(
            self.style().standardIcon(QtWidgets.QStyle.SP_BrowserReload),
            "Refresh",
        )
        self._refresh_button.clicked.connect(self._slot_refresh)
        self._top_bar_layout.addWidget(self._label_for_group_by)
        self._top_bar_layout.addWidget(self._group_by)
        self._top_bar_layout.addWidget(self._label_for_filter)
        self._top_bar_layout.addWidget(self._filter)
        self._top_bar_layout.addWidget(self._refresh_button)
        self._top_bar_layout.addStretch()
        self._root_layout.addLayout(self._top_bar_layout)

        self._bottom_part_layout = QtWidgets.QHBoxLayout()
        self._root_layout.addLayout(self._bottom_part_layout)

        self._tree = QTreeWidget()
        self._tree.setColumnCount(1)
        self._tree.setHeaderLabels(["Description", "Run", "Indexing", "Status"])

        _sample_based_tree(self._tree)

        self._bottom_part_layout.addWidget(self._tree)

    def _slot_refresh(self) -> None:
        pass

    def _slot_group_changed(self, newText: str) -> None:
        self._tree.clear()
        if newText == "Sample":
            _sample_based_tree(self._tree)
        else:
            _run_based_tree(self._tree)
