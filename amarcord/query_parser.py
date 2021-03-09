from typing import Dict, Any, Set, List, Callable, Tuple, Union
from lark import Lark, Token, Transformer, Tree
import lark.exceptions as le


def possibly_negate(negate: bool, c: bool) -> bool:
    return not c if negate else c


class SemanticError(Exception):
    pass


Field = Union[int, str, float, List[str], None]
Row = Dict[str, Field]
Query = Callable[[Row], bool]


def massage(field: Field, comparison: Field) -> Tuple[Field, Field]:
    if isinstance(field, int) and isinstance(comparison, str):
        try:
            return field, int(comparison)
        except:
            return str(field), comparison
    if isinstance(field, str) and isinstance(comparison, int):
        try:
            return int(field), comparison
        except:
            return field, str(comparison)
    if isinstance(field, float) and isinstance(comparison, str):
        try:
            return field, float(comparison)
        except:
            return str(field), comparison
    if isinstance(field, str) and isinstance(comparison, float):
        try:
            return float(field), comparison
        except:
            return field, str(comparison)
    return field, comparison


def to_python_operator(
    innerop: Tree,
    field: Token,
    comparison: Token,
    negate: bool,
) -> Query:
    innerop_child = innerop.children[0]
    assert isinstance(innerop_child, Token)
    op = innerop_child.value

    def with_row(row: Dict[str, Any]) -> bool:
        row_value = row[field.value]
        if not isinstance(row_value, (str, int, float, list)):
            raise SemanticError(
                f'row "{field.value}" has invalid type {type(row_value)}'
            )
        if isinstance(row_value, list) and not isinstance(
            row_value[0], (str, int, float)
        ):
            raise SemanticError(
                f'row "{field.value}" has invalid list type {type(row_value[0])}'
            )
        massaged_value, massaged_comparison = massage(row_value, comparison.value)
        if massaged_value is None:
            return True
        if op == "<":
            if isinstance(massaged_value, list) or isinstance(
                massaged_comparison, list
            ):
                raise SemanticError("'<' is not supported for lists")
            return possibly_negate(negate, massaged_value < massaged_comparison)  # type: ignore
        if op == "<=":
            if isinstance(massaged_value, list):
                raise SemanticError("'<=' is not supported for lists")
            return possibly_negate(negate, massaged_value <= massaged_comparison)  # type: ignore
        if op == ">":
            if isinstance(massaged_value, list):
                raise SemanticError("'>' is not supported for lists")
            return possibly_negate(negate, massaged_value > massaged_comparison)  # type: ignore
        if op == ">=":
            if isinstance(massaged_value, list):
                raise SemanticError("'>=' is not supported for lists")
            return possibly_negate(negate, massaged_value >= massaged_comparison)  # type: ignore
        if op == "!=":
            return possibly_negate(negate, massaged_value != massaged_comparison)  # type: ignore
        if op == "=":
            return possibly_negate(negate, massaged_value == massaged_comparison)  # type: ignore
        if op == "has":
            if not isinstance(massaged_value, (list, str)):
                raise SemanticError('"has" only works on lists of strings')
            if not isinstance(massaged_comparison, str):
                raise SemanticError('"has" only works on lists of strings')
            return possibly_negate(negate, massaged_comparison in massaged_value)
        return True

    return with_row


def and_then(first_function: Query, outerop: Tree, anothercondition: Query) -> Query:
    first_child = outerop.children[0]
    assert isinstance(first_child, Token)
    op = first_child.value
    if op == "and":
        return lambda row: first_function(row) and anothercondition(row)
    if op == "or":
        return lambda row: first_function(row) or anothercondition(row)
    raise Exception(f"invalid operator {op}")


class FieldNameError(Exception):
    def __init__(self, field: str, _field_names: Set[str]) -> None:
        super().__init__(f'Invalid field "{field}"')


class UnexpectedEOF(Exception):
    def __init__(self) -> None:
        super().__init__("Unexpected EOF")


class QueryToFunction(Transformer):
    def __init__(self, field_names: Set[str]) -> None:
        super().__init__()
        self._field_names = field_names

    def condition(self, s: List[Union[Tree, Token, Query]]) -> Any:
        assert isinstance(s[0], Token)
        base_index = 0 if s[0].type != "NOT" else 1
        field = s[base_index]
        assert isinstance(field, Token)
        if field.value not in self._field_names:
            raise FieldNameError(field.value, self._field_names)
        inner_operator = s[base_index + 1]
        assert isinstance(inner_operator, Tree)
        comparison = s[base_index + 2]
        assert isinstance(comparison, Token)
        first_function = to_python_operator(
            inner_operator, field, comparison, s[0].type == "NOT"
        )
        if len(s) == base_index + 3:
            return first_function
        outerop = s[base_index + 3]
        assert isinstance(outerop, Tree)
        remainder = s[base_index + 4]
        return and_then(first_function, outerop, remainder)  # type: ignore


query_parser = Lark(
    r"""
!condition: NOT? FIELD inneroperator COMPARISON (outeroperator condition)?
!outeroperator : AND | OR
!inneroperator : HAS | EQ | LT | GT | LE | GE | NE

NOT: "not"
HAS: "has"
AND: "and"
OR: "or"
EQ: "="
LT: "<"
GT: ">"
LE: "<="
GE: ">="
NE: "!="

FIELD : /[a-zA-Z_][a-zA-Z0-9_]*/
COMPARISON : /[a-zA-Z0-9_.-]+/
MYWORD : /[a-zA-Z]+/
%ignore " "
  """,
    start="condition",
)


def parse_query(query_string: str, field_names: Set[str]) -> Query:
    if not query_string:
        return lambda row: True
    # pylint: disable=global-statement
    global query_parser

    try:
        return QueryToFunction(field_names).transform(query_parser.parse(query_string))
    except le.UnexpectedEOF:
        # pylint: disable=raise-missing-from
        raise UnexpectedEOF()
    except Exception as e:
        if e.__context__ and isinstance(e.__context__, FieldNameError):
            raise e.__context__
        raise e


def filter_by_query(query: Query, row: Dict[str, Field]) -> bool:
    return query(row)
