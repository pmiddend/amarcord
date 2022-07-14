from dataclasses import dataclass
from functools import partial
from typing import Final, TypeAlias, Callable, Any, Dict

from lark import Lark, Transformer, Token

from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoTypeSample,
    AttributoType,
    AttributoTypeInt,
)
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.table_classes import DBRun
from amarcord.util import maybe_you_meant

_filter_expression_parser: Final = Lark(
    r"""
start: logical_expression

logical_expression: comparison_or_group (logical_operator comparison_or_group)*

comparison_or_group: "(" logical_expression ")" -> group
                   | comparison                 -> comparison_in_group

comparison: identifier_string comparison_operator atom

atom: STRING -> string
    | NUMBER -> number
    | "None" -> null
    | "True" -> true
    | "False" -> false
    | list_of_atoms

comparison_operator: "!="  -> op_neq
                   | "=="  -> op_eq
                   | ">"   -> op_gt
                   | "<"   -> op_lt
                   | "<="  -> op_le
                   | ">="  -> op_ge
                   | "?"   -> op_in

logical_operator: "and"    -> lop_and
                | "or"     -> lop_or

list_of_atoms: "[" (atom ("," atom)*)* "]"


%import common.ESCAPED_STRING   -> STRING
%import common.SIGNED_NUMBER    -> NUMBER

IDENTIFIER: /[a-zA-Z0-9_]+/

identifier_string: IDENTIFIER | STRING

// Disregard spaces in text
%ignore " "
"""
)


@dataclass(frozen=True)
class FilterInput:
    run: DBRun
    sample_names: Dict[str, int]


RunFilterFunction: TypeAlias = Callable[[FilterInput], bool]
ComparisonOperator = Callable[[Any, Any], bool]
LogicalOperator = Callable[[bool, bool], bool]
LogicalCombinator = Callable[[RunFilterFunction, RunFilterFunction], bool]


def _transform_comparison_operand_before_comparison(
    aid: AttributoId,
    input_: AttributoValue,
    type_: AttributoType,
    sample_names: dict[str, int],
) -> AttributoValue:
    if isinstance(type_, AttributoTypeSample):
        if isinstance(input_, (int, float)):
            if input_ not in sample_names.values():
                raise Exception(
                    f'attributo "{aid}": the sample ID {input_} is not known. You can also specify the sample name if you don\'t want to use the ID.'
                )
            return input_
        if isinstance(input_, str):
            sample_id_from_dict = sample_names.get(input_)
            if sample_id_from_dict is None:
                raise Exception(
                    f'sample with name "{input_}" not found{maybe_you_meant(input_, sample_names.keys())}'
                )
            return sample_id_from_dict
        raise Exception(
            f'attributo "{aid}" is of type "Sample ID", so we excepted a string (the sample name) or the sample ID. However, we got "{input_}".'
        )
    return input_


def _comparison_filter(
    attributo_id_raw: str,
    comparison_op: ComparisonOperator,
    operand: Any,
    filter_input: FilterInput,
) -> bool:
    run = filter_input.run
    aid = AttributoId(attributo_id_raw)
    attributo_value: AttributoValue
    type_: AttributoType
    if aid == AttributoId("id"):
        attributo_value = run.id
        type_ = AttributoTypeInt()
    else:
        attributo = run.attributi.retrieve_type(aid)
        if attributo is None:
            raise Exception(
                f'attributo "{aid}" not defined{maybe_you_meant(aid, run.attributi.names())}'
            )
        type_ = attributo.attributo_type
        attributo_value = run.attributi.select(aid)
    operand_processed = _transform_comparison_operand_before_comparison(
        aid, operand, type_, filter_input.sample_names
    )
    return comparison_op(attributo_value, operand_processed)


# noinspection PyMethodMayBeStatic
class MyTransformer(Transformer):
    def number(self, items: list[Token]) -> float:
        return float(items[0].value)

    def atom(self, items: list[Any]) -> Any:
        return items[0]

    def true(self, _items: list[Token]) -> bool:
        return True

    def string(self, items: list[Token]) -> str:
        # Remove the " at the beginning/end
        return items[0].value[1:-1]  # type: ignore

    def false(self, _items: list[Token]) -> bool:
        return False

    def null(self, _items: list[Token]) -> None:
        return None

    def list_of_atoms(self, items: Any) -> Any:
        return items

    def identifier_string(self, items: list[Token]) -> str:
        return items[0].value  # type: ignore

    def lop_and(self, _items: list[Token]) -> LogicalOperator:
        return lambda a, b: a and b

    def lop_or(self, _items: list[Token]) -> LogicalOperator:
        return lambda a, b: a or b

    def op_eq(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a == b  # type: ignore

    def op_neq(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a != b  # type: ignore

    def op_gt(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a > b if a is not None and b is not None else False  # type: ignore

    def op_lt(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a < b if a is not None and b is not None else False  # type: ignore

    def op_ge(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a >= b if a is not None and b is not None else False  # type: ignore

    def op_le(self, _items: list[Token]) -> ComparisonOperator:
        return lambda a, b: a <= b if a is not None and b is not None else False  # type: ignore

    def op_in(self, _items: list[Token]) -> ComparisonOperator:
        # Here we switch syntax since the filter query syntax is "attribute ? value" but "in" is "value in attribute"
        return lambda a, b: b in a

    def comparison(
        self, items: tuple[str, ComparisonOperator, Any]
    ) -> RunFilterFunction:
        return partial(_comparison_filter, items[0], items[1], items[2])

    def comparison_or_group(self, items: list[RunFilterFunction]) -> RunFilterFunction:
        return items[0].value  # type: ignore

    def group(self, items: list[Any]) -> RunFilterFunction:
        return items[0]  # type: ignore

    def comparison_in_group(self, items: list[Any]) -> RunFilterFunction:
        return items[0]  # type: ignore

    def start(self, items: list[Any]) -> RunFilterFunction:
        return items[0]  # type: ignore

    def logical_expression(self, items: list[Any]) -> RunFilterFunction:
        if len(items) == 1:
            return items[0]  # type: ignore

        def transformer(run: FilterInput) -> bool:
            result = items[0](run)
            for i in range(1, len(items), 2):
                logical_op = items[i]
                argument = items[i + 1](run)
                result = logical_op(result, argument)
            return result  # type: ignore

        return transformer


def compile_run_filter(query_string: str) -> RunFilterFunction:
    return MyTransformer().transform(_filter_expression_parser.parse(query_string))  # type: ignore
