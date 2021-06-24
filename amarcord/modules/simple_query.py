from lark import Lark

query_parser = Lark(
    r"""
!start : condition_chain*
!condition_chain : condition (outeroperator condition)?
!condition : FIELD operator VALUE
!operator : EQ | NE | LT | LE | GT | GE | IN
!outeroperator : AND

AND: "and"
EQ: "="
NE: "!="
LT: "<"
LE: "<="
GT: ">"
GE: ">="
IN: "in"

%ignore " "

FIELD : /[a-zA-Z_][a-zA-Z0-9_.]*/
VALUE : /[a-zA-Z_-+"]+/
  """,
    start="condition",
)


def query_to_sql(s: str) -> None:
    pass
