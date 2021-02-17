from typing import Dict, Any, Set, List, Callable, Tuple
from lark import Lark, Transformer
import lark.exceptions as le


def possibly_negate(negate: bool, c: bool) -> bool:
    return not c if negate else c


Row = Dict[str, Any]
Query = Callable[[Row], bool]


def massage(field: Any, comparison: Any) -> Tuple[Any, Any]:
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
    innerop: Any, field: Any, comparison: Any, negate: bool
) -> Query:
    op = innerop.children[0].value

    def with_row(row: Dict[str, Any]) -> bool:
        massaged_value, massaged_comparison = massage(
            row[field.value], comparison.value
        )
        if op == "<":
            return possibly_negate(negate, massaged_value < massaged_comparison)
        if op == "<=":
            return possibly_negate(negate, massaged_value <= massaged_comparison)
        if op == ">":
            return possibly_negate(negate, massaged_value > massaged_comparison)
        if op == ">=":
            return possibly_negate(negate, massaged_value >= massaged_comparison)
        if op == "!=":
            return possibly_negate(negate, massaged_value != massaged_comparison)
        if op == "=":
            return possibly_negate(negate, massaged_value == massaged_comparison)
        if op == "has":
            return possibly_negate(negate, massaged_comparison in massaged_value)
        return True

    return with_row


def and_then(first_function: Query, outerop: Any, anothercondition: Query) -> Query:
    op = outerop.children[0].value
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

    def condition(self, s: List[Any]) -> Any:
        base_index = 0 if s[0].type != "NOT" else 1
        field = s[base_index]
        if field not in self._field_names:
            raise FieldNameError(field, self._field_names)
        inneroperator = s[base_index + 1]
        comparison = s[base_index + 2]
        first_function = to_python_operator(
            inneroperator, field, comparison, s[0].type == "NOT"
        )
        if len(s) == base_index + 3:
            return first_function
        return and_then(first_function, s[base_index + 3], s[base_index + 4])


query_parser = Lark(
    r"""
!condition: NOT? FIELD inneroperator COMPARISON (outeroperator condition)?
!outeroperator : OR | AND
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
    except le.UnexpectedEOF as e:
        # pylint: disable=raise-missing-from
        raise UnexpectedEOF()
    except Exception as e:
        if e.__context__ and isinstance(e.__context__, FieldNameError):
            raise e.__context__
        raise e


def filter_by_query(query: Query, row: Dict[str, Any]) -> bool:
    return query(row)
