import datetime
from dataclasses import dataclass
from dataclasses import field
from decimal import Decimal
from typing import Iterable

import structlog
from openpyxl import Workbook
from openpyxl.cell import Cell
from openpyxl.cell import MergedCell
from openpyxl.cell.rich_text import CellRichText
from openpyxl.worksheet.formula import ArrayFormula
from openpyxl.worksheet.formula import DataTableFormula
from openpyxl.worksheet.worksheet import Worksheet

from amarcord.db import orm
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.orm_utils import data_sets_are_equal
from amarcord.db.orm_utils import run_has_attributo_to_data_set_has_attributo
from amarcord.db.run_external_id import RunExternalId
from amarcord.util import check_consecutive

logger = structlog.stdlib.get_logger(__name__)


@dataclass(frozen=True)
class ParsedRunSpreadsheetRun:
    original_row: int
    external_id: int
    experiment_type: str
    started: datetime.datetime
    stopped: None | datetime.datetime
    files: list[str]
    custom_column_values: list[str]


@dataclass(frozen=True)
class ParsedRunSpreadsheet:
    custom_column_headers: list[str]
    runs: list[ParsedRunSpreadsheetRun]


@dataclass(frozen=True)
class ConversionError:
    error_messages: list[str]

    def joined_messages(self) -> str:
        return ", ".join(self.error_messages)

    def is_empty(self) -> bool:
        return not self.error_messages

    @staticmethod
    def empty() -> "ConversionError":
        return ConversionError([])

    @staticmethod
    def singleton(message: str) -> "ConversionError":
        return ConversionError([message])

    def prepend(self, prefix: str) -> "ConversionError":
        return ConversionError([f"{prefix}: {s}" for s in self.error_messages])

    def combine(self, other: "ConversionError") -> "ConversionError":
        return ConversionError(self.error_messages + other.error_messages)


def excel_import_convert_cell_to_integer(
    cell_value: (
        None
        | ArrayFormula
        | CellRichText
        | DataTableFormula
        | datetime.date
        | datetime.time
        | datetime.timedelta
        | Decimal
        | float
        | str
    ),
) -> ConversionError | int:
    if cell_value is None:
        return ConversionError.singleton("doesn’t contain a value")
    if isinstance(cell_value, ArrayFormula):
        return ConversionError.singleton("cell contains an array formula")
    if isinstance(cell_value, CellRichText):
        return ConversionError.singleton("cell contains rich text")
    if isinstance(cell_value, DataTableFormula):
        return ConversionError.singleton("cell contains a data table formula")
    if isinstance(cell_value, datetime.date):
        return ConversionError.singleton("cell contains a date")
    if isinstance(cell_value, datetime.time):
        return ConversionError.singleton("cell contains a time")
    if isinstance(cell_value, datetime.timedelta):
        return ConversionError.singleton("cell contains a duration")
    try:
        return int(cell_value)
    except:
        return ConversionError.singleton(f'could not convert "{cell_value}" to integer')


def excel_import_convert_cell_to_string(
    cell_value: (
        None
        | ArrayFormula
        | CellRichText
        | DataTableFormula
        | datetime.date
        | datetime.time
        | datetime.timedelta
        | Decimal
        | float
        | str
    ),
) -> ConversionError | str:
    if cell_value is None:
        return ConversionError.singleton("doesn’t contain a value")
    if isinstance(cell_value, ArrayFormula):
        return ConversionError.singleton("cell contains an array formula")
    if isinstance(cell_value, CellRichText):
        return ConversionError.singleton("cell contains rich text")
    if isinstance(cell_value, DataTableFormula):
        return ConversionError.singleton("cell contains a data table formula")
    if isinstance(cell_value, datetime.date):
        return ConversionError.singleton("cell contains a date")
    if isinstance(cell_value, datetime.time):
        return ConversionError.singleton("cell contains a time")
    if isinstance(cell_value, datetime.timedelta):
        return ConversionError.singleton("cell contains a duration")
    return str(cell_value)


def _retrieve_custom_columns(
    ws: Worksheet, system_columns: int
) -> ConversionError | list[str]:
    custom_columns: list[str] = []
    current_column_idx = 0
    for column in ws.columns:
        first_cell = column[0]
        if isinstance(first_cell, MergedCell):
            return ConversionError(
                [
                    "found a column that starts with a “merged cell”, I don’t know how to handle that type of column"
                ]
            )
        current_column_idx += 1
        if current_column_idx <= system_columns:
            continue
        if not first_cell.value:
            return ConversionError(
                [f"column {first_cell.column_letter} has values, but no header"]
            )
        header = first_cell.value
        if not isinstance(header, str):
            return ConversionError(
                [
                    f"column {first_cell.column_letter} has a header that is not a string but {header}"
                ]
            )
        custom_columns.append(header)
    return custom_columns


_SYSTEM_COLUMNS = {
    "A": "run id",
    "B": "experiment type",
    "C": "started",
    "D": "stopped",
    "E": "files",
}


@dataclass(frozen=True)
class RunCellResult:
    run_id: None | int = None
    experiment_type_name: None | str = None
    started: None | datetime.datetime = None
    stopped: None | datetime.datetime = None
    files: None | list[str] = None
    custom_column_values: list[str] = field(default_factory=list)

    def combine(self, other: "RunCellResult") -> "RunCellResult":
        return RunCellResult(
            run_id=self.run_id if self.run_id is not None else other.run_id,
            experiment_type_name=(
                self.experiment_type_name
                if self.experiment_type_name is not None
                else other.experiment_type_name
            ),
            started=self.started if self.started is not None else other.started,
            stopped=self.stopped if self.stopped is not None else other.stopped,
            files=self.files if self.files is not None else other.files,
            custom_column_values=self.custom_column_values + other.custom_column_values,
        )

    def build_run(self, row_idx: int) -> ConversionError | ParsedRunSpreadsheetRun:
        if self.run_id is None:
            return ConversionError.singleton("run ID is missing")
        if self.experiment_type_name is None:
            return ConversionError.singleton("experiment type is missing")
        if self.started is None:
            return ConversionError.singleton("started is missing")
        if self.files is None:
            return ConversionError.singleton("files are missing")
        return ParsedRunSpreadsheetRun(
            original_row=row_idx,
            external_id=self.run_id,
            experiment_type=self.experiment_type_name,
            started=self.started,
            stopped=self.stopped,
            custom_column_values=self.custom_column_values,
            files=self.files,
        )

    @staticmethod
    def empty() -> "RunCellResult":
        return RunCellResult(
            run_id=None,
            experiment_type_name=None,
            started=None,
            stopped=None,
            files=None,
            custom_column_values=[],
        )


def _parse_run_row_cell(
    cell_idx: int, cell: Cell | MergedCell
) -> RunCellResult | ConversionError:
    if cell_idx == 0:
        converted_run_id = excel_import_convert_cell_to_integer(cell.value)
        if isinstance(converted_run_id, ConversionError):
            return ConversionError.singleton(
                f"should be a numerical run ID; however: {converted_run_id.joined_messages()}"
            )
        return RunCellResult(run_id=converted_run_id)
    if cell_idx == 1:
        experiment_type_name_converted = excel_import_convert_cell_to_string(cell.value)
        if isinstance(experiment_type_name_converted, ConversionError):
            return ConversionError.singleton(
                f"should be a string; however: {experiment_type_name_converted.joined_messages()}"
            )
        if not experiment_type_name_converted.strip():
            return ConversionError.singleton(
                "should be the experiment type name, but it's empty"
            )
        return RunCellResult(experiment_type_name=experiment_type_name_converted)
    if cell_idx == 2:
        started_cell = cell.value
        if not isinstance(started_cell, datetime.datetime):
            return ConversionError.singleton(
                f'should the run start time, but it\'s not a valid time stamp: "{started_cell}"'
            )
        return RunCellResult(started=started_cell)
    if cell_idx == 3:
        stopped_cell = cell.value
        if stopped_cell:
            if not isinstance(stopped_cell, datetime.datetime):
                return ConversionError.singleton(
                    f'should the run stop time, but it\'s not a valid time stamp: "{stopped_cell}"'
                )
            return RunCellResult(stopped=stopped_cell)
        return RunCellResult(stopped=None)
    if cell_idx == 4:
        if not isinstance(cell.value, str):
            return ConversionError.singleton(
                f'should the list of files for this run, but it\'s not a string: "{cell.value}"'
            )
        return RunCellResult(files=[v.strip() for v in cell.value.split(",")])
    return RunCellResult(
        custom_column_values=[str(cell.value) if cell.value is not None else ""]
    )


def _parse_run_row(
    row_idx: int, row: Iterable[Cell | MergedCell]
) -> ConversionError | ParsedRunSpreadsheetRun:
    row_result: RunCellResult = RunCellResult.empty()
    cell_errors: ConversionError = ConversionError.empty()
    for cell_idx, cell in enumerate(row):
        cell_result = _parse_run_row_cell(cell_idx, cell)
        if isinstance(cell_result, ConversionError):
            cell_errors = cell_errors.combine(cell_result.prepend(f"{cell.coordinate}"))
            continue
        row_result = row_result.combine(cell_result)

    if not cell_errors.is_empty():
        return cell_errors

    built_run = row_result.build_run(row_idx)
    if isinstance(built_run, ConversionError):
        return built_run.prepend(f"row {row_idx}")
    return built_run


def parse_run_spreadsheet_workbook(
    contents: Workbook,
) -> ConversionError | ParsedRunSpreadsheet:
    ws = contents.active

    if ws is None:
        raise Exception("workbook does not have an active worksheet (how can that be?)")

    column_header_errors: list[str] = []

    for letter, expected_header in _SYSTEM_COLUMNS.items():
        header_value = ws[f"{letter}1"].value
        if header_value != expected_header:
            column_header_errors.append(
                f'expected column "{letter}1" to be "{expected_header}", but it\'s "{header_value}"'
            )

    if column_header_errors:
        return ConversionError(column_header_errors)

    custom_columns = _retrieve_custom_columns(ws, len(_SYSTEM_COLUMNS))

    if isinstance(custom_columns, ConversionError):
        return custom_columns

    runs: list[ParsedRunSpreadsheetRun] = []
    conversion_errors = ConversionError.empty()
    row_idx = 1
    for row in ws.iter_rows(min_row=2):
        row_idx += 1
        row_result = _parse_run_row(row_idx, row)
        if isinstance(row_result, ConversionError):
            conversion_errors = conversion_errors.combine(row_result)
        else:
            runs.append(row_result)

    if not conversion_errors.is_empty():
        return conversion_errors

    return ParsedRunSpreadsheet(custom_column_headers=custom_columns, runs=runs)


@dataclass(frozen=True)
class SpreadsheetValidationErrors:
    errors: list[str]


def convert_value(
    a: orm.Attributo,
    chemicals_by_name: dict[str, orm.Chemical],
    value: str | int | float | datetime.datetime,
) -> None | ConversionError | orm.RunHasAttributoValue:
    atype = schema_dict_to_attributo_type(a.json_schema)
    if isinstance(atype, AttributoTypeInt):
        # Simple: we want an integer, but the value is empty. This is fine, return None.
        if isinstance(value, str) and not value.strip():
            return None
        if isinstance(value, int):
            return orm.RunHasAttributoValue(
                attributo_id=a.id,
                integer_value=value,
                float_value=None,
                string_value=None,
                bool_value=None,
                datetime_value=None,
                list_value=None,
                chemical_value=None,
            )
        if isinstance(value, str | float):
            try:
                return orm.RunHasAttributoValue(
                    attributo_id=a.id,
                    integer_value=int(value),
                    float_value=None,
                    string_value=None,
                    bool_value=None,
                    datetime_value=None,
                    list_value=None,
                    chemical_value=None,
                )
            except:
                return ConversionError.singleton(
                    f'is an integer, but value is "{value}", cannot convert'
                )
        return ConversionError.singleton(
            f'is an integer, but value is "{value}", cannot convert'
        )
    if isinstance(atype, AttributoTypeDecimal):
        if isinstance(value, str) and not value.strip():
            return None
        if isinstance(value, float | int):
            real_value = float(value)
        elif isinstance(value, str):
            try:
                real_value = float(value)
            except:
                return ConversionError.singleton(
                    f'is a decimal, but value is "{value}", cannot convert'
                )
        else:
            return ConversionError.singleton(
                f'is a decimal integer, but value is "{value}", cannot convert'
            )

        if atype.range is not None and not atype.range.value_is_inside(real_value):
            return ConversionError.singleton(
                f'value "{value}" is outside range {atype.range}'
            )

        return orm.RunHasAttributoValue(
            attributo_id=a.id,
            integer_value=None,
            float_value=real_value,
            string_value=None,
            bool_value=None,
            datetime_value=None,
            list_value=None,
            chemical_value=None,
        )
    if isinstance(atype, AttributoTypeChemical):
        if not isinstance(value, str):
            return ConversionError.singleton(
                f'is a chemical, but value is "{value}", cannot convert (I expect the chemical name)'
            )
        name = value.strip()
        if not name:
            return None
        chemical = chemicals_by_name.get(name)
        if chemical is None:
            return ConversionError.singleton(
                f"is a chemical, but did not find the chemical “{name}”, maybe you forgot to add it? we have the following chemicals: "
                + ", ".join(f"“{n}”" for n in chemicals_by_name)
            )
        return orm.RunHasAttributoValue(
            attributo_id=a.id,
            integer_value=None,
            float_value=None,
            string_value=None,
            bool_value=None,
            datetime_value=None,
            list_value=None,
            chemical_value=chemical.id,
        )
    if isinstance(atype, AttributoTypeString):
        if not isinstance(value, str | int | float):
            return ConversionError.singleton(
                f'is a string, but value is "{value}", cannot convert this'
            )
        return orm.RunHasAttributoValue(
            attributo_id=a.id,
            integer_value=None,
            float_value=None,
            string_value=str(value),
            bool_value=None,
            datetime_value=None,
            list_value=None,
            chemical_value=None,
        )
    if isinstance(atype, AttributoTypeBoolean):
        if not isinstance(value, str):
            return ConversionError.singleton(
                f'is a boolean, but value is "{value}", cannot convert this (has to be a string yes/no)'
            )
        return orm.RunHasAttributoValue(
            attributo_id=a.id,
            integer_value=None,
            float_value=None,
            string_value=None,
            bool_value=value == "yes",
            datetime_value=None,
            list_value=None,
            chemical_value=None,
        )
    if isinstance(atype, AttributoTypeDateTime):
        return ConversionError.singleton(
            "date/time attributi are not supported right now"
        )
    if isinstance(atype, AttributoTypeChoice):
        if not isinstance(value, str):
            return ConversionError.singleton(
                f'is a choice of strings, but value is "{value}", cannot convert this'
            )
        if not value.strip():
            return None
        if value.strip() not in atype.values:
            return ConversionError.singleton(
                f'value is "{value}", this is not in the list of choice values '
                + ", ".join(atype.values)
            )

        return orm.RunHasAttributoValue(
            attributo_id=a.id,
            integer_value=None,
            float_value=None,
            string_value=value,
            bool_value=None,
            datetime_value=None,
            list_value=None,
            chemical_value=None,
        )
    return ConversionError.singleton(f"invalid attributo type for import: {atype}")


@dataclass(frozen=True)
class SpreadsheetValidationResult:
    runs: list[orm.Run]
    warnings: list[str]


def create_runs_from_spreadsheet(
    spreadsheet: ParsedRunSpreadsheet,
    beamtime_id: BeamtimeId,
    attributi: list[orm.Attributo],
    chemicals: list[orm.Chemical],
    existing_runs: list[orm.Run],
    experiment_types: list[orm.ExperimentType],
) -> SpreadsheetValidationErrors | SpreadsheetValidationResult:
    if not experiment_types:
        return SpreadsheetValidationErrors(
            errors=["couldn't find any experiment types"]
        )
    attributi_by_name: dict[str, orm.Attributo] = {a.name: a for a in attributi}
    invalid_custom_columns: list[str] = []
    custom_column_attributi: list[orm.Attributo] = []
    for custom_column_header in spreadsheet.custom_column_headers:
        attributo = attributi_by_name.get(custom_column_header)
        if attributo is None:
            invalid_custom_columns.append(custom_column_header)
        else:
            custom_column_attributi.append(attributo)
    if invalid_custom_columns:
        return SpreadsheetValidationErrors(
            errors=[
                "the following columns were found in the spreadsheet, but we have no attributi for them: "
                + ", ".join(invalid_custom_columns)
            ]
        )

    experiment_types_by_name: dict[str, orm.ExperimentType] = {
        et.name: et for et in experiment_types
    }
    errors = ConversionError.empty()
    orm_runs: list[orm.Run] = []
    existing_file_globs_with_rows: dict[str, int] = {}
    warnings: list[str] = []
    run_ids: set[int] = set(x.external_id for x in existing_runs)
    additional_run_ids_with_rows: dict[int, int] = {}
    for run in spreadsheet.runs:
        et = experiment_types_by_name.get(run.experiment_type)
        if et is None:
            errors = errors.combine(
                ConversionError.singleton(
                    f"row {run.original_row}: experiment type “{run.experiment_type}” not found in list of experiment types (this is case-sensitive!)"
                )
            )
            continue

        if run.external_id in run_ids:
            errors = errors.combine(
                ConversionError.singleton(
                    f"row {run.original_row}: you already have a run with ID {run.external_id} in the database"
                )
            )
            continue

        existing_run_with_this_id = additional_run_ids_with_rows.get(run.external_id)
        if existing_run_with_this_id is not None:
            errors = errors.combine(
                ConversionError.singleton(
                    f"row {run.original_row}: you already have a run with ID {run.external_id} in row {existing_run_with_this_id}"
                )
            )
            continue

        additional_run_ids_with_rows[run.external_id] = run.original_row

        attributo_values: list[orm.RunHasAttributoValue] = []
        chemicals_by_name: dict[str, orm.Chemical] = {
            c.name.strip(): c for c in chemicals
        }
        for attributo, attributo_value in zip(
            custom_column_attributi, run.custom_column_values, strict=False
        ):
            attributo_result = convert_value(
                attributo, chemicals_by_name, attributo_value
            )
            if isinstance(attributo_result, ConversionError):
                errors = errors.combine(
                    attributo_result.prepend(
                        f'row {run.original_row}: column "{attributo.name}"'
                    )
                )
            elif attributo_result is not None:
                attributo_values.append(attributo_result)

        for f in run.files:
            row_for_existing_file = existing_file_globs_with_rows.get(f)
            if row_for_existing_file is not None:
                warnings.append(
                    f"row {run.original_row}: the file path {f} already appears in row {row_for_existing_file} - is this deliberate?"
                )
            else:
                existing_file_globs_with_rows[f] = run.original_row

        new_run = orm.Run(
            external_id=RunExternalId(run.external_id),
            experiment_type_id=et.id,
            beamtime_id=beamtime_id,
            started=run.started,
            stopped=run.stopped,
            modified=datetime.datetime.now(datetime.timezone.utc),
            files=[orm.RunHasFiles(glob=f, source="raw") for f in run.files],
            attributo_values=attributo_values,
        )
        new_run.experiment_type = et
        orm_runs.append(new_run)
    nonconsecutive_run_id_pair = check_consecutive(
        sorted(additional_run_ids_with_rows.keys())
    )
    if nonconsecutive_run_id_pair is not None:
        from_, to_ = nonconsecutive_run_id_pair
        warnings.append(
            f"run IDs are not consecutive, we have a jump from run ID {from_} to run ID {to_}; you can still import the spreadsheet, but this run ID mixup could be a mistake?"
        )
    if not errors.is_empty():
        return SpreadsheetValidationErrors(errors=errors.error_messages)
    return SpreadsheetValidationResult(runs=orm_runs, warnings=warnings)


def create_data_set_for_runs(
    ets: list[orm.ExperimentType],
    runs: list[orm.Run],
    existing_data_sets: list[orm.DataSet],
) -> list[orm.DataSet] | ConversionError:
    result: list[orm.DataSet] = []
    et_per_id: dict[int, orm.ExperimentType] = {et.id: et for et in ets}
    error_messages: list[str] = []
    for run in runs:
        et = et_per_id.get(run.experiment_type_id)
        if et is None:
            raise Exception(
                f"run with unknown experiment type ID {run.experiment_type_id}"
            )
        new_data_set = orm.DataSet(experiment_type_id=run.experiment_type_id)
        for et_attributo in run.experiment_type.attributi:
            for run_attributo in run.attributo_values:
                if run_attributo.attributo_id == et_attributo.attributo_id:
                    new_data_set.attributo_values.append(
                        run_has_attributo_to_data_set_has_attributo(run_attributo)
                    )
        if len(new_data_set.attributo_values) != len(run.experiment_type.attributi):
            for existing_attributo in run.experiment_type.attributi:
                found = False
                for data_set_attributo in new_data_set.attributo_values:
                    if (
                        data_set_attributo.attributo_id
                        == existing_attributo.attributo_id
                    ):
                        found = True
                        break
                if not found:
                    error_messages.append(
                        f"run {run.external_id}: tried to create a data set for experiment type “{run.experiment_type.name}”, but attributo “{existing_attributo.attributo.name}” not found in this run"
                    )
        if not new_data_set.attributo_values:
            error_messages.append(
                f"run {run.external_id}: found no attributo values to create the data set"
            )
        have_equal = False
        for previous_ds in result:
            if data_sets_are_equal(previous_ds, new_data_set):
                have_equal = True
        for existing_ds in existing_data_sets:
            if data_sets_are_equal(existing_ds, new_data_set):
                have_equal = True
        if not have_equal:
            result.append(new_data_set)
    if error_messages:
        return ConversionError(error_messages)
    return result
