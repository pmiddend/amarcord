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
from amarcord.db.db import DBSample
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.tables import create_tables
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
        ):
            print(f"skipping because special attributo: {random_attributo.name}")
            return
        new_value = generate_attributo(
            [cast(int, s.id) for s in db.retrieve_samples(conn, None)], random_attributo
        )

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
        db.add_run(
            conn,
            PROPOSAL_ID,
            rid,
            random.choice(samples).id if samples else None,
            generate_attributi(
                [cast(int, s.id) for s in samples],
                db.retrieve_table_attributi(conn, AssociatedTable.RUN),
            ),
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


def generate_random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))


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


def generate_attributo(sample_ids: List[int], amd: DBAttributo) -> AttributoValue:
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
        return random.choice(sample_ids) if sample_ids else None
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
    sample_ids: List[int], param: Dict[AttributoId, DBAttributo]
) -> RawAttributiMap:
    manual_source: JSONDict = {}
    result: JSONDict = {MANUAL_SOURCE_NAME: manual_source}
    for aid, amd in param.items():
        if aid == AttributoId("id") or aid == AttributoId("proposal_id"):
            continue
        new_value = generate_attributo(sample_ids, amd)
        if new_value is not None:
            manual_source[str(aid)] = new_value

    return RawAttributiMap(result)


def action_add_sample() -> None:
    with db.connect() as conn:
        targets = db.retrieve_targets(conn)
        db.add_sample(
            conn,
            DBSample(
                id=None,
                target_id=random.choice(targets).id,
                compounds=None,
                micrograph=None,
                protocol=None,
                attributi=generate_attributi(
                    db.retrieve_sample_ids(conn),
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
            random_attributo = random.choice(list(attributi.values()))
            new_value = generate_attributo(
                [cast(int, sample.id) for sample in samples], random_attributo
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


actions_with_weights = [
    (action_change_run_property, 50),
    (action_add_run, 15),
    (action_remove_run, 3),
    (action_add_comment, 5),
    (action_add_sample, 3),
    (action_modify_sample, 3),
    (action_add_attributi, 1),
]

while True:
    random.choices(
        [x[0] for x in actions_with_weights], [x[1] for x in actions_with_weights], k=1
    )[0]()
    sleep(1)
