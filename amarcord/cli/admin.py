import logging
import sys
from getpass import getpass
from typing import Optional
from typing import cast

from tap import Tap

from amarcord.db.alembic import upgrade_to_head
from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.sample_data import add_xfel_2696_attributi
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import DBContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class SwitchAdminPassword(Tap):
    proposal_id: int  # Proposal ID to change the password for


class RemoveAdminPassword(Tap):
    proposal_id: int  # Proposal ID to remove the password for


class AddProposal(Tap):
    proposal_id: int  # Proposal ID
    password: Optional[str] = None  # Password for the proposal


class Migrate(Tap):
    pass


class Add2696Attributi(Tap):
    pass


class Arguments(Tap):
    connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)

    """Various helper tools for the AMARCORD administrator"""

    def configure(self) -> None:
        self.add_subparsers(dest="subparser_name")
        self.add_subparser(
            "switch-admin-password",
            SwitchAdminPassword,
            help="Switch the administrator password for a proposal",
        )
        self.add_subparser(
            "remove-admin-password",
            RemoveAdminPassword,
            help="Remove the administrator password for a proposal",
        )
        self.add_subparser(
            "add-proposal",
            AddProposal,
            help="Add a new proposal",
        )
        self.add_subparser(
            "add-2696-attributi",
            Add2696Attributi,
            help="Add attributi specific to the 2696 XFEL proposal",
        )
        self.add_subparser(
            "migrate",
            Migrate,
            help="Migrate the DB to the latest version",
        )


def main() -> int:
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = DBContext(args.connection_url)
    db = DB(dbcontext, create_tables(dbcontext))
    # pylint: disable=no-member
    subparser_name = args.subparser_name  # type: ignore
    if subparser_name == "migrate":
        upgrade_to_head(args.connection_url)
        logger.info("Migration finished successfully!")
    elif subparser_name == "add-2696-attributi":
        with db.connect() as conn:
            add_xfel_2696_attributi(db, conn)
            logger.info("Attributi successfully inserted!")
    elif subparser_name == "remove-admin-password":
        subargs_remove = cast(RemoveAdminPassword, args)
        with db.connect() as conn:
            db.change_proposal_password(
                conn,
                # pylint: disable=no-member
                ProposalId(subargs_remove.proposal_id),
                None,
            )
            logger.info("Password successfully removed!")
    elif subparser_name == "switch-admin-password":
        subargs_switch = cast(SwitchAdminPassword, args)
        new_password = getpass()
        with db.connect() as conn:
            db.change_proposal_password(
                conn,
                # pylint: disable=no-member
                ProposalId(subargs_switch.proposal_id),
                new_password,
            )
            logger.info("Password successfully switched!")
    elif subparser_name == "add-proposal":
        subargs_add = cast(AddProposal, args)
        with db.connect() as conn:
            db.add_proposal(
                conn,
                # pylint: disable=no-member
                ProposalId(subargs_add.proposal_id),
                subargs_add.password,
            )
            logger.info("Proposal added!")
    else:
        logger.warning("No command given!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
