from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.tables import DBTables
from amarcord.newdb.tables import SeparateSchemata

dbcontext = DBContext("mysql+pymysql://root:root@localhost/SARS_COV_2_v2")
db = NewDB(
    dbcontext,
    DBTables(
        dbcontext.metadata,
        with_tools=False,
        with_estimated_resolution=False,
        schemata=SeparateSchemata("SARS_COV_2_v2", "SARS_COV_2_Analysis_v2"),
        engine=dbcontext.engine,
    ),
)

with db.connect() as conn:
    i = 0
    for r in db.retrieve_refinements(conn):
        print(r)
        if i > 10:
            break
        i += 1
    i = 0
    for c in db.retrieve_crystals(conn):
        print(c)
        if i > 10:
            break
        i += 1
    results = db.retrieve_analysis_results(
        conn, filter_query="", sort_column=None, sort_order_desc=False, limit=100
    )
    print(results)
