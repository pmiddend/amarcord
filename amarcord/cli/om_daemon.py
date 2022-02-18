import asyncio
import logging

from tap import Tap
from zmq.asyncio import Context

from amarcord.amici.om.zeromq import OmZMQProcessor
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.dbcontext import CreationMode
from amarcord.db.tables import create_tables_from_metadata

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    om_url: str
    topic: str


async def main_loop():
    args = Arguments(underscores_to_dashes=True).parse_args()

    dbcontext = AsyncDBContext(args.db_connection_url)

    tables = create_tables_from_metadata(dbcontext.metadata)

    if args.db_connection_url.startswith("sqlite://"):
        await dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    processor = OmZMQProcessor(AsyncDB(dbcontext, tables))
    await processor.init()

    zmq_ctx = Context.instance()
    await processor.main_loop(zmq_ctx, args.om_url, args.topic)


asyncio.run(main_loop())
