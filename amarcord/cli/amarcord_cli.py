import asyncio

from tap import Tap

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.tables import create_tables_from_metadata


class Arguments(Tap):
    db_connection_url: str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)


async def _main_loop(args: Arguments) -> None:
    db_context = AsyncDBContext(args.db_connection_url)
    db = AsyncDB(db_context, create_tables_from_metadata(db_context.metadata))
    await db.migrate()

    async with db.begin() as conn:
        attributi = await db.retrieve_attributi(
            conn, associated_table=AssociatedTable.RUN
        )
        await db.create_run(
            conn,
            1,
            attributi,
            attributi_map=AttributiMap.from_types_and_json(
                attributi,
                await db.retrieve_sample_ids(conn),
                {ATTRIBUTO_STARTED: 1657321200000},
            ),
            keep_manual_attributes_from_previous_run=False,
        )
    #     await db.create_indexing_result(
    #         conn,
    #         DBIndexingResult(
    #             id=None,
    #             created=datetime.datetime.utcnow(),
    #             run_id=1,
    #             frames=0,
    #             hits=0,
    #             not_indexed_frames=0,
    #             runtime_status=DBIndexingResultDone(
    #                 stream_file=Path(
    #                     "/gpfs/cfel/user/pmidden/amarcord-merging-test/input.stream"
    #                 ),
    #                 job_error=None,
    #                 fom=empty_indexing_fom,
    #             ),
    #         ),
    #     )


def main() -> None:
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
