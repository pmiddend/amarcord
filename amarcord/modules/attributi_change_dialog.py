from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set

from PyQt5.QtWidgets import QDialog
from PyQt5.QtWidgets import QDialogButtonBox
from PyQt5.QtWidgets import QHBoxLayout
from PyQt5.QtWidgets import QVBoxLayout
from PyQt5.QtWidgets import QWidget

from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBRun
from amarcord.db.dbcontext import Connection
from amarcord.json import JSONValue
from amarcord.json import json_make_immutable
from amarcord.modules.spb.attributi_table import AttributiTable


def attributi_from_runs(
    attributi: List[AttributoId], runs: Iterable[DBRun]
) -> RawAttributiMap:
    assert runs

    values_per_attributo: Dict[AttributoId, Set[JSONValue]] = {}
    for run in runs:
        for attributo in attributi:
            value_ = run.attributi.select(attributo)
            value = json_make_immutable(value_.value) if value_ is not None else None
            if attributo not in values_per_attributo:
                values_per_attributo[attributo] = {value}
            else:
                values_per_attributo[attributo].add(value)

    result = RawAttributiMap({})
    for aid, values in values_per_attributo.items():
        if len(values) == 1:
            v = next(iter(values))
            if v is not None:
                result.append_single_to_source(ONLINE_SOURCE_NAME, aid, v)

    return result


@dataclass(frozen=False)
class ChangeDialogResult:
    run_ids: Set[int]
    new_values: Dict[AttributoId, Any]
    manual_removed: Set[AttributoId]


class _AttributiChangeData:
    def __init__(
        self,
        runs: List[DBRun],
        metadata: Dict[AttributoId, DBAttributo],
        samples: List[DBMiniSample],
        result: ChangeDialogResult,
    ) -> None:
        self.attributi_table = AttributiTable(
            attributi_from_runs(list(metadata.keys()), runs),
            metadata,
            samples,
            self._attributi_changed,
            self._manual_removed,
        )
        self._result = result

    def _attributi_changed(self, id_: AttributoId, value: Any) -> None:
        self._result.new_values[id_] = value
        self._result.manual_removed.discard(id_)

        self.attributi_table.set_single_manual(id_, value)

    def _manual_removed(self, id_: AttributoId) -> None:
        self._result.manual_removed.add(id_)
        self._result.new_values.pop(id_)

        self.attributi_table.remove_manual(id_)


def attributi_change_dialog(
    runs: List[DBRun],
    metadata: Dict[AttributoId, DBAttributo],
    samples: List[DBMiniSample],
    parent: Optional[QWidget] = None,
) -> Optional[ChangeDialogResult]:
    dialog = QDialog(parent)
    dialog.setWindowTitle("Change attributi")
    dialog_layout = QVBoxLayout(dialog)

    description_row = QHBoxLayout()
    dialog_layout.addLayout(description_row)

    result = ChangeDialogResult(
        run_ids={x.id for x in runs}, new_values={}, manual_removed=set()
    )

    change_data = _AttributiChangeData(runs, metadata, samples, result)

    dialog_layout.addWidget(change_data.attributi_table)

    buttonBox = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    buttonBox.accepted.connect(dialog.accept)
    buttonBox.rejected.connect(dialog.reject)

    dialog_layout.addWidget(buttonBox)

    if dialog.exec() == QDialog.Rejected:
        return None

    return result


def apply_run_changes(db: DB, conn: Connection, changes: ChangeDialogResult) -> None:
    for run_id in changes.run_ids:
        for aid in changes.manual_removed:
            db.update_run_attributo(conn, run_id, aid, None)
        for aid, v in changes.new_values.items():
            db.update_run_attributo(conn, run_id, aid, v)
