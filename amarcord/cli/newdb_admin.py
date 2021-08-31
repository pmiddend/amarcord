import logging
import sys
from typing import cast

from tap import Tap

from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.tables import DBTables

CREATE_INITIAL = "create-initial"

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class CreateInitial(Tap):
    check_first: bool = True  # Check if the database tables exist before creating them
    with_tools: bool = True  # Create tools tables as well (jobs, ...)
    with_estimated_resolution: bool = (
        True  # Create Diffraction table with estimated_resolution column
    )


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # Echo SQL statements

    """Various helper tools for the AMARCORD administrator (new db version)"""

    def configure(self) -> None:
        self.add_subparsers(dest="subparser_name")
        self.add_subparser(
            CREATE_INITIAL,
            CreateInitial,
            help="Create an initial database",
        )


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.db_connection_url, args.db_echo)
    # pylint: disable=no-member
    subparser_name = args.subparser_name  # type: ignore

    if subparser_name == CREATE_INITIAL:
        create_initial_args = cast(CreateInitial, args)
        logger.info("Creating initial database")
        NewDB(
            dbcontext,
            DBTables(
                dbcontext.metadata,
                create_initial_args.with_tools,
                create_initial_args.with_estimated_resolution,
                None,
            ),
        )
        dbcontext.create_all(
            CreationMode.CHECK_FIRST
            if create_initial_args.check_first
            else CreationMode.DONT_CHECK
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
