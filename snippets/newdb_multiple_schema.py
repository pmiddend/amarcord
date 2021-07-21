from amarcord.modules.dbcontext import DBContext
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.tables import DBTables

dbcontext = DBContext("mysql+pymysql://root:root@localhost/SARS_COV_2_v2")
db = NewDB(
    dbcontext,
    DBTables(
        dbcontext.metadata,
        with_tools=False,
        with_estimated_resolution=False,
        normal_schema=None,
        analysis_schema="SARS_COV_2_Analysis_v2",
    ),
)

with db.connect() as conn:
    results = db.retrieve_analysis_results(
        conn, filter_query="", sort_column=None, sort_order_desc=False, limit=100
    )
    print(results)
