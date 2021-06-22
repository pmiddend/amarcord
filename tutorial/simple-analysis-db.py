from typing import Final

from amarcord.db.db import DB
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext

PROPOSAL_ID: Final = ProposalId(1)

# Create a temporary test database in /tmp
dbcontext = DBContext("sqlite:////tmp/test-database.sqlite")
db = DB(dbcontext, create_tables(dbcontext))
dbcontext.create_all(CreationMode.CHECK_FIRST)

# Open a transaction
with db.connect() as conn:
    # Create a new dummy proposal (runs need proposals)
    db.add_proposal(conn, PROPOSAL_ID)

    # Create a new run without any attributes and no sample in it
    # using our proposal from above
    run_id = 1
    db.add_run(conn, PROPOSAL_ID, run_id, sample_id=None, attributi=RawAttributiMap({}))

    # Add a data source - returns the data source ID
    # "source" signifies the source files and is a dictionary. There's no
    # fixed format for this right now.
    ds_id = db.add_data_source(
        conn,
        DBDataSource(
            id=None,
            run_id=run_id,
            number_of_frames=1,
            source={},
            tag="test",
            comment=None,
        ),
    )

    # Add peak search parameters. Has lots of parameters, most of them optional (check the whole
    # DBPeakSearchParameters class!).
    psp_id = db.add_peak_search_parameters(
        conn, DBPeakSearchParameters(id=None, method="peakfinder8", software="CrystFEL")
    )
    hfp_id = db.add_hit_finding_parameters(
        conn,
        DBHitFindingParameters(
            id=None,
            min_peaks=1,
            tag="test-tag",
            comment=None,
            software="CrystFEL",
            software_version="0.9.1",
        ),
    )
    hfr_id = db.add_hit_finding_result(
        conn,
        DBHitFindingResult(
            id=None,
            peak_search_parameters_id=psp_id,
            hit_finding_parameters_id=hfp_id,
            data_source_id=ds_id,
            result_filename="temp.stream",
            peaks_filename=None,
            result_type="stream",
            average_peaks_event=1.0,
            average_resolution=1.0,
            number_of_hits=1,
            hit_rate=0.3,
            tag=None,
            comment=None,
        ),
    )
    ip_id = db.add_indexing_parameters(
        conn,
        DBIndexingParameters(
            id=None,
            tag=None,
            comment=None,
            software="CrystFEL",
            software_version="0.9.1",
            command_line="...",
            parameters={},
            methods=[],
            geometry=None,
        ),
    )
    intp_id = db.add_integration_parameters(
        conn,
        DBIntegrationParameters(
            id=None,
            tag=None,
            comment=None,
            software="CrystFEL",
            software_version="0.9.1",
            method=None,
            center_boxes=None,
            overpredict=None,
            push_res=None,
            radius_inner=None,
            radius_middle=None,
            radius_outer=None,
        ),
    )
    db.add_indexing_result(
        conn,
        DBIndexingResult(
            id=None,
            hit_finding_result_id=hfr_id,
            peak_search_parameters_id=psp_id,
            indexing_parameters_id=ip_id,
            integration_parameters_id=intp_id,
            num_indexed=1,
            num_crystals=1,
            tag=None,
            comment=None,
            result_filename=None,
        ),
    )

print("done")
