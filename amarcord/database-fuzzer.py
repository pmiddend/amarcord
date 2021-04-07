import datetime
import random
import string
import sys
from time import sleep
from typing import Dict
from typing import List
from typing import cast

from isodate import duration_isoformat

from amarcord.config import load_user_config
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeDuration
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_type import AttributoTypeTags
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.db import DB
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.mini_sample import DBMiniSample
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingParameters
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBIndexingParameters
from amarcord.db.table_classes import DBIndexingResult
from amarcord.db.table_classes import DBIntegrationParameters
from amarcord.db.table_classes import DBPeakSearchParameters
from amarcord.db.table_classes import DBSample
from amarcord.db.table_classes import DBTarget
from amarcord.db.tables import create_tables
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.numeric_range import NumericRange
from amarcord.numeric_range import random_from_range

PROPOSAL_ID = ProposalId(1)

user_config = load_user_config()

if user_config["db_url"] is None:
    sys.exit(1)

db_context = DBContext(user_config["db_url"])
db = DB(db_context, create_tables(db_context))
db_context.create_all(CreationMode.CHECK_FIRST)

with db.connect() as global_conn:
    if not db.have_proposals(global_conn):
        db.add_proposal(global_conn, PROPOSAL_ID)
        db.add_attributo(
            global_conn, "first_train", "", AssociatedTable.RUN, AttributoTypeInt()
        )
        db.add_attributo(
            global_conn, "last_train", "", AssociatedTable.RUN, AttributoTypeInt()
        )
        db.add_target(
            global_conn,
            DBTarget(
                id=None,
                name="target name",
                short_name="target short name",
                molecular_weight=None,
                uniprot_id="",
            ),
        )


def action_change_run_property() -> None:
    with db.connect() as conn:
        run_ids = db.retrieve_run_ids(conn, PROPOSAL_ID)

        if not run_ids:
            print("no run IDs, cannot change property")
            return

        random_run_id = random.choice(run_ids)

        attributi = db.retrieve_table_attributi(conn, AssociatedTable.RUN)
        random_attributo = random.choice(list(attributi.values()))
        if (
            random_attributo.name == AttributoId("id")
            or random_attributo.name == AttributoId("proposal_id")
            or random_attributo.name == AttributoId("comments")
            or random_attributo.name == AttributoId("modified")
            or random_attributo.name == AttributoId("first_train")
            or random_attributo.name == AttributoId("last_train")
        ):
            print(f"skipping because special attributo: {random_attributo.name}")
            return
        new_value = generate_attributo(db.retrieve_mini_samples(conn), random_attributo)

        print(
            f"changing run {random_run_id}'s attributo {random_attributo.name} to {new_value}"
        )
        db.update_run_attributo(conn, random_run_id, random_attributo.name, new_value)


def action_add_run() -> None:
    with db.connect() as conn:
        prior_run_ids = db.retrieve_run_ids(conn, PROPOSAL_ID)

        samples = db.retrieve_samples(conn, None)

        rid = max(prior_run_ids, default=0) + 1
        print(f"adding run {rid}")
        run_attributi = generate_attributi(
            db.retrieve_mini_samples(conn),
            db.retrieve_table_attributi(conn, AssociatedTable.RUN),
        )
        run_attributi.set_single_manual(AttributoId("first_train"), 0)
        run_attributi.set_single_manual(
            AttributoId("last_train"), random.randint(600, 30000)
        )
        db.add_run(
            conn,
            PROPOSAL_ID,
            rid,
            random.choice(samples).id if samples else None,
            run_attributi,
        )


def action_remove_run() -> None:
    with db.connect() as conn:
        run_ids = db.retrieve_run_ids(conn, PROPOSAL_ID)

        rid = random.choice(run_ids)
        print(f"Deleting run {rid}...")
        db.delete_run(conn, rid)


def action_add_comment() -> None:
    with db.connect() as conn:
        run_ids = db.retrieve_run_ids(conn, PROPOSAL_ID)

        rid = random.choice(run_ids)
        print(f"Adding comment to run {rid}")
        db.add_comment(conn, rid, generate_random_string(), generate_random_string())


def generate_attributo_int() -> AttributoTypeInt:
    return AttributoTypeInt()


def generate_range() -> NumericRange:
    a, b = random.random(), random.random()

    if random.random() > 0.25:
        return NumericRange(None, False, None, False)

    return NumericRange(
        min(a, b), random.random() < 0.5, max(a, b), random.random() < 0.5
    )


def generate_random_string(max_length=32) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(max_length))


def generate_attributo_double() -> AttributoTypeDouble:
    return AttributoTypeDouble(generate_range(), generate_random_string(), False)


def generate_attributo_string() -> AttributoTypeString:
    return AttributoTypeString()


def generate_attributo_duration() -> AttributoTypeDuration:
    return AttributoTypeDuration()


def generate_attributo_type() -> AttributoType:
    attributo_choices = [
        generate_attributo_int,
        generate_attributo_double,
        generate_attributo_string,
        generate_attributo_duration,
    ]
    return random.choice(attributo_choices)()  # type: ignore


def generate_attributo(samples: List[DBMiniSample], amd: DBAttributo) -> AttributoValue:
    if isinstance(amd.attributo_type, AttributoTypeInt):
        return random.randint(0, 99999)
    if isinstance(amd.attributo_type, AttributoTypeChoice):
        return random.choice([v[1] for v in amd.attributo_type.values])
    if isinstance(amd.attributo_type, AttributoTypeDouble):
        if amd.attributo_type.range is not None:
            return random_from_range(amd.attributo_type.range)
        return random.uniform(-10000, 10000)
    if isinstance(amd.attributo_type, AttributoTypeString):
        return generate_random_string()
    if isinstance(amd.attributo_type, AttributoTypeSample):
        return random.choice(samples).sample_id if samples else None
    if isinstance(amd.attributo_type, AttributoTypeDuration):
        return duration_isoformat(
            datetime.timedelta(
                days=random.uniform(0, 10), seconds=random.uniform(0, 86399)
            )
        )
    if isinstance(amd.attributo_type, AttributoTypeTags):
        return [generate_random_string() for _ in range(random.randrange(0, 10))]
    return None


def generate_attributi(
    samples: List[DBMiniSample], param: Dict[AttributoId, DBAttributo]
) -> RawAttributiMap:
    manual_source: JSONDict = {}
    result: JSONDict = {MANUAL_SOURCE_NAME: manual_source}
    for aid, amd in param.items():
        if aid == AttributoId("id") or aid == AttributoId("proposal_id"):
            continue
        new_value = generate_attributo(samples, amd)
        if new_value is not None:
            manual_source[str(aid)] = new_value

    return RawAttributiMap(result)


def action_add_event() -> None:
    with db.connect() as conn:
        db.add_event(
            conn,
            EventLogLevel.INFO,
            generate_random_string(10),
            generate_random_string(),
            datetime.datetime.utcnow(),
        )


def action_add_sample() -> None:
    with db.connect() as conn:
        targets = db.retrieve_targets(conn)
        db.add_sample(
            conn,
            DBSample(
                id=None,
                name=generate_random_string(),
                target_id=random.choice(targets).id,
                compounds=None,
                micrograph=None,
                protocol=None,
                attributi=generate_attributi(
                    db.retrieve_mini_samples(conn),
                    db.retrieve_table_attributi(conn, AssociatedTable.SAMPLE),
                ),
            ),
        )


def action_modify_sample() -> None:
    with db.connect() as conn:
        samples = db.retrieve_samples(conn, None)
        if samples:
            s = random.choice(samples)
            attributi = db.retrieve_table_attributi(conn, AssociatedTable.SAMPLE)
            attributi.pop(AttributoId("id"), None)
            random_attributo = random.choice(list(attributi.values()))
            new_value = generate_attributo(
                db.retrieve_mini_samples(conn), random_attributo
            )
            print(f"sample {s.id}: setting {random_attributo.name} to {new_value}")
            db.update_sample_attributo(
                conn, cast(int, s.id), random_attributo.name, new_value
            )


def action_add_attributi() -> None:
    with db.connect() as conn:
        t = random.choice([AssociatedTable.SAMPLE, AssociatedTable.RUN])
        number_attributi = len(db.retrieve_table_attributi(conn, t).values())
        attributo_name = f"att{number_attributi}"
        print(f"Adding attributo {attributo_name} to {t.name}")
        db.add_attributo(
            conn, attributo_name, generate_random_string(), t, generate_attributo_type()
        )


def generate_peak_search_parameters() -> DBPeakSearchParameters:
    return DBPeakSearchParameters(
        id=None,
        method=generate_random_string(8),
        software=generate_random_string(),
        command_line=generate_random_string(),
        tag=generate_random_string(8),
        comment=generate_random_string(),
        software_version=generate_random_string(4),
        software_git_sha=generate_random_string(),
        max_num_peaks=random.uniform(0, 100),
        adc_threshold=random.uniform(0, 100),
        minimum_snr=random.uniform(0, 100),
        min_pixel_count=random.uniform(0, 100),
        max_pixel_count=random.uniform(0, 100),
        min_res=random.uniform(0, 100),
        max_res=random.uniform(0, 100),
        bad_pixel_filename=generate_random_string(),
        local_bg_radius=random.uniform(0, 100),
        min_peak_over_neighbor=random.uniform(0, 100),
        min_snr_biggest_pix=random.uniform(0, 100),
        min_snr_peak_pix=random.uniform(0, 100),
        min_sig=random.uniform(0, 100),
        min_squared_gradient=random.uniform(0, 100),
        geometry_filename=generate_random_string(),
    )


def generate_hit_finding_parameters() -> DBHitFindingParameters:
    return DBHitFindingParameters(
        id=None,
        min_peaks=10,
        tag=generate_random_string(8),
        comment=generate_random_string(),
    )


def generate_hit_finding_result(
    number_of_frames: int, data_source_id: int
) -> DBHitFindingResult:
    number_of_hits = random.randint(0, number_of_frames)
    return DBHitFindingResult(
        id=None,
        data_source_id=data_source_id,
        peak_search_parameters=generate_peak_search_parameters(),
        hit_finding_parameters=generate_hit_finding_parameters(),
        result_filename=generate_random_string(),
        number_of_hits=number_of_hits,
        hit_rate=number_of_hits / number_of_frames * 100,
        indexing_results=[],
        tag=generate_random_string(8),
        comment=generate_random_string(),
    )


def generate_indexing_result(hfr_id: int, number_of_hits: int) -> DBIndexingResult:
    return DBIndexingResult(
        id=None,
        hit_finding_results_id=hfr_id,
        indexing_parameters=DBIndexingParameters(
            software=generate_random_string(),
            command_line=generate_random_string(),
            parameters={},
        ),
        integration_parameters=DBIntegrationParameters(),
        ambiguity_parameters=None,
        num_indexed=random.randint(0, number_of_hits),
        num_crystals=1,
        tag=generate_random_string(8),
        comment=generate_random_string(),
    )


def action_add_data_source() -> None:
    with db.connect() as conn:
        number_of_frames = random.randint(10, 1000)
        run_ids = db.retrieve_run_ids(conn, PROPOSAL_ID)
        if not run_ids:
            print("cannot add data source, no runs yet")
            return
        data_source_id = db.add_data_source(
            conn,
            DBDataSource(
                id=None,
                run_id=random.choice(run_ids),
                number_of_frames=number_of_frames,
                hit_finding_results=[],
                source={
                    "files": [
                        generate_random_string() for _ in range(random.randint(1, 50))
                    ]
                },
                tag=generate_random_string(max_length=8),
                comment=generate_random_string(),
            ),
        )

        for hfr in (
            generate_hit_finding_result(number_of_frames, data_source_id)
            for _ in range(random.randint(0, 2))
        ):
            hfr_id = db.add_hit_finding_result(conn, hfr)

            for ir in (
                generate_indexing_result(hfr_id, hfr.number_of_hits)
                for _ in range(random.randint(0, 2))
            ):
                db.add_indexing_result(conn, ir)


actions_with_weights = [
    (action_change_run_property, 50),
    (action_add_run, 15),
    (action_add_data_source, 5),
    # Commented out temporarily because we cannot delete run "1" in SQLite for some shitty reason
    # (action_remove_run, 3),
    (action_add_sample, 3),
    (action_add_event, 3),
    (action_modify_sample, 3),
    (action_add_comment, 1),
    (action_add_attributi, 1),
]

while True:
    random.choices(
        [x[0] for x in actions_with_weights], [x[1] for x in actions_with_weights], k=1
    )[0]()
    sleep(1)
