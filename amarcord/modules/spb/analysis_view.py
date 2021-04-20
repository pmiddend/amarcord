import json
from typing import Any
from typing import Dict
from typing import Final
from typing import List
from typing import Optional
from typing import TypeVar
from typing import Union

from PyQt5.QtCore import QModelIndex
from PyQt5.QtCore import QTimer
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QAbstractItemView
from PyQt5.QtWidgets import QCheckBox
from PyQt5.QtWidgets import QGroupBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QHeaderView
from PyQt5.QtWidgets import QPushButton
from PyQt5.QtWidgets import QSplitter
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QTreeWidget
from PyQt5.QtWidgets import QTreeWidgetItem
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBLinkedIndexingResult
from amarcord.db.table_classes import DBSampleAnalysisResult
from amarcord.db.tables import DBTables
from amarcord.modules.context import Context
from amarcord.modules.spb.analysis_tree import TreeItem
from amarcord.modules.spb.analysis_tree import TreeNode
from amarcord.modules.spb.analysis_tree import build_analysis_tree
from amarcord.modules.spb.analysis_tree import runs_to_number_of_trains
from amarcord.qt.declarative_table import Column
from amarcord.qt.declarative_table import Data
from amarcord.qt.declarative_table import DeclarativeTable
from amarcord.qt.declarative_table import Row
from amarcord.qt.signal_blocker import SignalBlocker

AUTO_REFRESH_TIMER_MSEC: Final = 5000


def _format_percent(f: Optional[float]) -> str:
    return f"{f:.2f}%" if f is not None else ""


T = TypeVar("T")


def _empty_if_none(x: Optional[T]) -> str:
    return "" if x is None else str(x)


def _detail_columns():
    return [
        Column(header_label="Name", editable=False),
        Column(header_label="Value", editable=False, stretch=True),
    ]


class _AnalysisTree(QTreeWidget):
    # This function ensures that when we clear and re-fill the tree, we don't scroll to the currently selected
    # item. One might want this, but we don't.
    def scrollTo(
        self,
        index: QModelIndex,
        hint: QAbstractItemView.ScrollHint = QAbstractItemView.EnsureVisible,
    ) -> None:
        if hint == QAbstractItemView.EnsureVisible:
            return
        super().scrollTo(index, hint)


def _to_qt_tree(k: TreeNode) -> QTreeWidgetItem:
    return QTreeWidgetItem(
        [
            k.description,
            f"{k.duration_minutes}min" if k.duration_minutes is not None else "",
            str(k.number_of_frames) if k.number_of_frames is not None else "",
            k.tag if k.tag is not None else "",
            _format_percent(k.hit_rate),
            _format_percent(k.indexing_rate),
        ]
    )


def _build_tree(
    k: TreeNode,
    current_item: TreeItem,
    expanded_items: List[TreeItem],
    parent: QTreeWidgetItem,
) -> Optional[QTreeWidgetItem]:
    qt_tree = _to_qt_tree(k)
    qt_tree.setData(0, Qt.UserRole, k.value)

    parent.addChild(qt_tree)

    qt_tree.setExpanded(any(_is_same_item(k.value, b) for b in expanded_items))

    result = qt_tree if _is_same_item(k.value, current_item) else None

    for k_child in k.children:
        child_is_current = _build_tree(k_child, current_item, expanded_items, qt_tree)
        if result is None and child_is_current:
            result = child_is_current

    return result


def _is_same_item(a: TreeItem, b: TreeItem) -> bool:
    if isinstance(a, DBSampleAnalysisResult) and isinstance(b, DBSampleAnalysisResult):
        return a.sample_id == b.sample_id
    if isinstance(a, DBLinkedDataSource) and isinstance(b, DBLinkedDataSource):
        return a.data_source.id == b.data_source.id
    if isinstance(a, DBLinkedHitFindingResult) and isinstance(
        b, DBLinkedHitFindingResult
    ):
        return a.hit_finding_result.id == b.hit_finding_result.id
    if isinstance(a, DBLinkedIndexingResult) and isinstance(b, DBLinkedIndexingResult):
        return a.indexing_result.id == b.indexing_result.id
    return False


def _item_to_prop_dict(item: TreeItem) -> Dict[str, str]:
    if isinstance(item, DBLinkedDataSource):
        source_content = (
            "\n".join(str(s) for s in item.data_source.source["files"])  # type: ignore
            if isinstance(item.data_source.source, dict)
            and "files" in item.data_source.source
            else json.dumps(item.data_source.source)
        )
        return {
            "Comment": item.data_source.comment,  # type: ignore
            "Number of frames": str(item.data_source.number_of_frames),
            "Source files": source_content if item.data_source.source else "",
        }
    if isinstance(item, DBLinkedHitFindingResult):
        psp = item.peak_search_parameters
        return {
            "Hit Finding.Comment": item.hit_finding_result.comment,  # type: ignore
            "Hit Finding.Number of hits": item.hit_finding_result.number_of_hits,  # type: ignore
            "Hit Finding.Result filename": item.hit_finding_result.result_filename.replace(
                ",", "\n"
            ),
            "Hit Finding.Result peaks file": item.hit_finding_result.peaks_filename,  # type: ignore
            "Hit Finding.Minimum peaks": str(item.hit_finding_parameters.min_peaks),
            "Peak Search.Tag": _empty_if_none(psp.tag),
            "Peak Search.Comment": _empty_if_none(psp.comment),
            "Peak Search.Software": psp.software,
            "Peak Search.Max Num Peaks": str(psp.max_num_peaks),
            "Peak Search.ADC Threshold": str(psp.adc_threshold),
            "Peak Search.Minimum SNR": str(psp.minimum_snr),
            "Peak Search.Minimum Pixel Count": str(psp.min_pixel_count),
            "Peak Search.Maximum Pixel Count": str(psp.max_pixel_count),
            "Peak Search.Minimum Res": str(psp.min_res),
            "Peak Search.Maximum Res": str(psp.max_res),
            "Peak Search.Bad Pixel Filename": _empty_if_none(
                psp.bad_pixel_map_filename
            ),
            "Peak Search.Local BG Radius": str(psp.local_bg_radius),
            "Peak Search.Min Peak Over Neighbor": str(psp.min_peak_over_neighbor),
            "Peak Search.Min SNR Biggest Pix": str(psp.min_snr_biggest_pix),
            "Peak Search.Min SNR Peak Pix": str(psp.min_snr_peak_pix),
            "Peak Search.Min sig": str(psp.min_sig),
            "Peak Search.Min Squared Gradient": str(psp.min_squared_gradient),
        }
    if isinstance(item, DBLinkedIndexingResult):
        return {
            "Indexing.Tag": _empty_if_none(item.indexing_result.tag),
            "Indexing.Comment": _empty_if_none(item.indexing_result.comment),
            "Indexing.Num Indexed": str(item.indexing_result.num_indexed),
            "Indexing.Num Crystals": str(item.indexing_result.num_crystals),
            "Indexing.Software": item.indexing_parameters.software,
            "Indexing.Command Line": item.indexing_parameters.command_line,
            "Indexing.Parameters": json.dumps(item.indexing_parameters.parameters),
        }
    return {}


class AnalysisView(QWidget):
    def __init__(
        self,
        context: Context,
        tables: DBTables,
        proposal_id: ProposalId,
        parent: Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)

        self._proposal_id = proposal_id
        self._db = DB(context.db, tables)
        self._current_item: Union[
            None, DBLinkedDataSource, DBLinkedIndexingResult, DBLinkedHitFindingResult
        ] = None

        root_layout = QVBoxLayout(self)

        root_splitter = QSplitter(Qt.Vertical)

        refresh_line_layout = QHBoxLayout()
        refresh_button = QPushButton(
            self.style().standardIcon(QStyle.SP_BrowserReload),
            "Refresh",
        )
        refresh_line_layout.addWidget(refresh_button)
        refresh_button.clicked.connect(self._slot_refresh_with_conn)
        self._auto_refresh = QCheckBox("Auto refresh")
        self._auto_refresh.setChecked(True)
        self._auto_refresh.toggled.connect(self._slot_toggle_auto_refresh)
        refresh_line_layout.addWidget(self._auto_refresh)
        refresh_line_layout.addStretch()

        root_layout.addLayout(refresh_line_layout)

        self._tree = _AnalysisTree()
        self._tree.setColumnCount(3)
        self._tree.setHeaderLabels(
            ["", "Duration", "No. of frames", "Tag", "Hit rate", "Index Rate"]
        )
        self._tree.header().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tree.itemExpanded.connect(self._item_expanded)
        self._tree.itemCollapsed.connect(self._item_collapsed)

        self._expanded_items: List[
            Union[
                DBLinkedDataSource,
                DBSampleAnalysisResult,
                DBLinkedHitFindingResult,
                DBLinkedIndexingResult,
            ]
        ] = []
        # self._refresh_tree()

        root_splitter.addWidget(self._tree)
        self._tree.currentItemChanged.connect(self._current_item_changed)

        group_box = QGroupBox("Metadata for selected item")
        group_box_layout = QHBoxLayout(group_box)
        self._metadata_table = DeclarativeTable(
            Data(
                rows=[],
                columns=[],
                column_delegates={},
                row_delegates={},
                resize_rows=True,
            )
        )
        group_box_layout.addWidget(self._metadata_table)
        root_splitter.addWidget(group_box)
        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._slot_refresh_with_conn)

        root_layout.addWidget(root_splitter)
        root_splitter.setStretchFactor(0, 2)

    def _item_expanded(self, item: QTreeWidgetItem) -> None:
        self._expanded_items.append(item.data(0, Qt.UserRole))

    def _item_collapsed(self, item: QTreeWidgetItem) -> None:
        y = item.data(0, Qt.UserRole)
        self._expanded_items = [
            x for x in self._expanded_items if not _is_same_item(x, y)
        ]

    def _refresh_tree(self):
        # So we don't emit the "current item changed" signal
        with SignalBlocker(self._tree):
            self._tree.clear()
        with self._db.connect() as conn:
            new_current_item: Optional[QTreeWidgetItem] = None

            for k in build_analysis_tree(
                self._db.retrieve_sample_based_analysis(conn),
                runs_to_number_of_trains(
                    self._db.retrieve_runs(conn, self._proposal_id, since=None)
                ),
            ):
                top_item = _to_qt_tree(k)
                top_item.setData(0, Qt.UserRole, k.value)
                self._tree.addTopLevelItem(top_item)

                if _is_same_item(self._current_item, k.value):
                    new_current_item = top_item

                top_item.setExpanded(
                    any(_is_same_item(k.value, b) for b in self._expanded_items)
                )

                for k_child in k.children:
                    child_result = _build_tree(
                        k_child, self._current_item, self._expanded_items, top_item
                    )
                    if child_result is not None:
                        new_current_item = child_result

            self._tree.setCurrentItem(new_current_item)

    def _current_item_changed(self, current_raw: Optional[QTreeWidgetItem]) -> None:
        if current_raw is None:
            self._current_item = None
            self._metadata_table.set_data(
                Data(
                    rows=[],
                    columns=_detail_columns(),
                    row_delegates={},
                    column_delegates={},
                    resize_rows=True,
                )
            )
            return

        self._current_item = current_raw.data(0, Qt.UserRole)

        if self._current_item is None:
            return

        props = _item_to_prop_dict(self._current_item)

        self._metadata_table.set_data(
            Data(
                rows=[
                    Row(
                        display_roles=[k, v if v is not None else ""],
                        edit_roles=[None, None],
                    )
                    for k, v in props.items()
                ],
                columns=_detail_columns(),
                row_delegates={},
                column_delegates={},
                resize_rows=True,
            )
        )

    def _slot_refresh_with_conn(self) -> None:
        self._refresh_tree()

    def _slot_toggle_auto_refresh(self) -> None:
        if self._update_timer.isActive():
            self._update_timer.stop()
        else:
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)

    def hideEvent(self, _e: Any) -> None:
        self._update_timer.stop()

    def showEvent(self, _e: Any) -> None:
        if self._auto_refresh.isChecked():
            self._slot_refresh_with_conn()
            self._update_timer.start(AUTO_REFRESH_TIMER_MSEC)
