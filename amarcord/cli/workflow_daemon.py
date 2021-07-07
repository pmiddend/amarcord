import logging
import sys
from time import sleep

from tap import Tap

from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_job_to_reduction
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db import table_reduction_jobs
from amarcord.amici.p11.db import table_tools
from amarcord.modules.dbcontext import DBContext
from amarcord.workflows.job_controller_factory import create_job_controller
from amarcord.workflows.job_controller_factory import parse_job_controller
from amarcord.workflows.workflow_synchronize import check_jobs

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    db_echo: bool = False  # output SQL statements?
    wait_after_check: float = (
        1.0  # Frequency in seconds to wait after a successfully synchronization
    )
    job_controller: str  # Job controller string (has a defined, documented format)


def main(args: Arguments) -> int:
    dbcontext = DBContext(args.db_connection_url, echo=args.db_echo)

    table_tools_ = table_tools(dbcontext.metadata)
    table_pucks_ = table_pucks(dbcontext.metadata)
    table_crystals_ = table_crystals(dbcontext.metadata, table_pucks_)
    table_diffractions_ = table_diffractions(dbcontext.metadata, table_crystals_)
    table_reduction_jobs_ = table_reduction_jobs(
        dbcontext.metadata, table_tools_, table_diffractions_
    )
    table_data_reduction_ = table_data_reduction(dbcontext.metadata, table_crystals_)
    table_job_to_reductions_ = table_job_to_reduction(
        dbcontext.metadata, table_reduction_jobs_, table_data_reduction_
    )

    job_controller = create_job_controller(parse_job_controller(args.job_controller))

    logger.info("starting synchronizing jobs")
    try:
        while True:
            sleep(args.wait_after_check)

            with dbcontext.connect() as conn:
                check_jobs(
                    job_controller,
                    conn,
                    table_tools_,
                    table_reduction_jobs_,
                    table_job_to_reductions_,
                    table_diffractions_,
                    table_data_reduction_,
                )
    except:
        logger.exception("caught an unexpected exception")
        return 1


if __name__ == "__main__":
    sys.exit(main(Arguments(underscores_to_dashes=True).parse_args()))
