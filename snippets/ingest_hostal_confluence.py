import logging
import re
import sys
from dataclasses import dataclass
from typing import List
from typing import Optional
from typing import Tuple

import sqlalchemy as sa
from atlassian import Confluence
from bs4 import BeautifulSoup
from bs4 import Tag
from tap import Tap

from amarcord.amici.p11.db import PuckType
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_dewar_lut
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

NAME_COLUMN = "Name"
POSITION_COLUMN = "Position"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Crystal:
    position: int
    name: str


@dataclass(frozen=True)
class Puck:
    puck_id: str
    crystals: List[Crystal]


def parse_crystals(table: Tag, puck_id: str) -> Tuple[List[Crystal], bool]:
    column_name_to_index = {
        "".join(th.stripped_strings): idx for idx, th in enumerate(table.find_all("th"))
    }

    if POSITION_COLUMN not in column_name_to_index:
        logger.warning('couldn\'t find column "Position" in table for puck %s', puck_id)
        return [], True

    if NAME_COLUMN not in column_name_to_index:
        logger.warning('couldn\'t find column "Name" in table for puck %s', puck_id)
        return [], True

    crystals: List[Crystal] = []
    has_warnings = False
    for row_idx, row in enumerate(table.find_all("tr")[1:]):
        cols = row.find_all("td")
        position_str = "".join(
            cols[column_name_to_index[POSITION_COLUMN]].stripped_strings
        )
        try:
            position = int(position_str)
        except:
            logger.warning(
                "puck %s, row %s: position %s is not an integer",
                puck_id,
                row_idx,
                position_str,
            )
            has_warnings = True
            continue

        if position in [c.position for c in crystals]:
            logger.warning(
                "puck %s, row %s: position %s already encountered",
                puck_id,
                row_idx,
                position,
            )
            has_warnings = True
            continue

        name = "".join(cols[column_name_to_index[NAME_COLUMN]].stripped_strings)

        crystals.append(Crystal(position, name))

    return crystals, has_warnings


def parse_pucks(soup: Tag) -> Tuple[List[Puck], bool]:
    pucks: List[Puck] = []
    has_warnings = False
    for headline in soup.find_all("h1"):
        headline_text = "".join(headline.strings)
        headline_match = re.match(r".*Puck ID:\s*(.+)", headline_text)
        if not headline_match:
            continue
        puck_id = headline_match.group(1)
        if not puck_id:
            logger.warning('Found "Puck ID:", but nothing after that in %s', puck_id)
            has_warnings = True
            continue
        headline_sibling = headline
        table, table_has_warnings = find_puck_table(headline_sibling, puck_id)
        has_warnings = has_warnings or table_has_warnings
        if table is None:
            logger.warning(
                "found puck headline %s, but no table afterwards",
                puck_id,
            )
            has_warnings = True
            continue

        crystals, crystal_has_warnings = parse_crystals(table, puck_id)

        has_warnings = has_warnings or crystal_has_warnings

        if crystals:
            pucks.append(Puck(puck_id, crystals))
    return pucks, has_warnings


def find_puck_table(headline_sibling: Tag, puck_id: str) -> Tuple[Optional[Tag], bool]:
    has_warnings = False
    table = None
    while table is None:
        headline_sibling = headline_sibling.next_sibling
        if headline_sibling.name == "h1":
            logger.warning(
                "found puck headline %s, but no table afterwards (until the next headline)",
                puck_id,
            )
            has_warnings = True
            continue
        if headline_sibling.name == "table":
            table = headline_sibling
    return table, has_warnings


class Arguments(Tap):
    """Ingest Hostal Confluence tables into the DB"""

    db_connection_url: Optional[
        str
    ] = None  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    ignore_warnings: bool = False
    confluence_username: str
    confluence_password: str
    confluence_page_id: int
    confluence_url: str = "https://confluence.desy.de"
    db_echo: bool = False  # output SQL statements?


def create_crystal_id(crystal: Crystal, max_crystal_id: int) -> str:
    # Strip away characters that cause trouble on filesystems
    modified_name = re.sub(r"[^A-Za-z0-9_-]", "", crystal.name)
    return f"{modified_name}_pos{crystal.position}_{max_crystal_id}"


def ingest_puck(
    conn: Connection,
    pucks: sa.Table,
    crystals: sa.Table,
    puck: Puck,
    max_crystal_id: int,
) -> int:
    # Only ingest if it's not already there (checkin the puck ID here)
    if (
        conn.execute(
            sa.select([sa.func.count(pucks.c.puck_id)]).where(
                pucks.c.puck_id == puck.puck_id
            )
        ).fetchone()[0]
        == 0
    ):
        logger.info("Inserting puck %s", puck.puck_id)
        conn.execute(
            sa.insert(pucks).values(
                puck_id=puck.puck_id, puck_type=PuckType.UNI, owner="Janina"
            )
        )
    else:
        logger.info("Skipping puck %s creation, already there", puck.puck_id)
    for crystal in puck.crystals:
        if (
            conn.execute(
                sa.select([sa.func.count(crystals.c.crystal_id)]).where(
                    sa.and_(
                        crystals.c.puck_id == puck.puck_id,
                        crystals.c.puck_position_id == crystal.position,
                    )
                )
            ).fetchone()[0]
            == 0
        ):
            crystal_id = create_crystal_id(crystal, max_crystal_id)
            logger.info("Inserting crystal %s", crystal_id)
            conn.execute(
                sa.insert(crystals).values(
                    crystal_id=crystal_id,
                    puck_id=puck.puck_id,
                    puck_position_id=crystal.position,
                )
            )
            max_crystal_id += 1
        else:
            logger.info(
                "Skipping crystal creation for puck %s, position %s, already there",
                puck.puck_id,
                crystal.position,
            )
    return max_crystal_id


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    confluence = Confluence(
        url=args.confluence_url,
        username=args.confluence_username,
        password=args.confluence_password,
    )

    pucks, has_warnings = parse_pucks(
        BeautifulSoup(
            confluence.get_page_by_id(args.confluence_page_id, expand="body.storage")[
                "body"
            ]["storage"]["value"],
            features="lxml",
        )
    )

    if has_warnings and not args.ignore_warnings:
        logger.warning(
            "There were warnings. Study them carefully and then use --ignore-warnings to ingest anyway"
        )
        return 1

    if args.db_connection_url is not None:
        dbcontext = DBContext(args.db_connection_url, echo=args.db_echo)
        crystals_table = table_crystals(dbcontext.metadata)
        pucks_table = table_pucks(dbcontext.metadata)
        # Currently unused, but stated here so it gets created also
        table_dewar_lut(dbcontext.metadata)
        table_diffractions(dbcontext.metadata)
        table_data_reduction(dbcontext.metadata)

        dbcontext.create_all(CreationMode.CHECK_FIRST)

        with dbcontext.connect() as conn:
            with conn.begin():
                max_crystal_id_row = conn.execute(
                    sa.select([sa.func.count(crystals_table.c.crystal_id)])
                ).fetchone()
                max_crystal_id = (
                    1 if max_crystal_id_row is None else max_crystal_id_row[0]
                )
                for puck in pucks:
                    max_crystal_id = ingest_puck(
                        conn, pucks_table, crystals_table, puck, max_crystal_id + 1
                    )
    else:
        for puck in pucks:
            logger.info("puck %s, crystals:", puck.puck_id)
            for c in puck.crystals:
                logger.info("crystal %s, %s", c.name, c.position)

    return 0


if __name__ == "__main__":
    sys.exit(main())
