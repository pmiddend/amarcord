import datetime
from random import choice
from random import randint
from random import random
from random import randrange
from random import seed
from random import uniform
from typing import Final
from typing import List

import bcrypt

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.constants import DB_SOURCE_NAME
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.tables import DBTables
from amarcord.db.tables import logger
from amarcord.modules.dbcontext import DBContext


def create_sample_data(context: DBContext, tables: DBTables) -> None:
    logger.info("Creating sample data...")
    with context.connect() as conn:
        proposal_id = 1

        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw("foobar".encode("utf-8"), salt)

        # Create some events
        conn.execute(
            tables.event_log.insert().values(
                source="karabo",
                text="test line please ignore",
                level=EventLogLevel.INFO,
            )
        )

        # Create proposal
        conn.execute(
            tables.proposal.insert().values(
                id=proposal_id,
                metadata={"data": {}, "title": "test proposal"},
                admin_password=hashed_password.decode("utf-8"),
            )
        )
        # Second proposal in case you want to test the proposal chooser
        # conn.execute(
        #     tables.proposal.insert().values(
        #         id=2, metadata={"data": {}, "title": "shit proposal"}
        #     )
        # )
        # Create targets
        first_target_id = conn.execute(
            tables.target.insert().values(name="Main Protease", short_name="MPro")
        ).inserted_primary_key[0]
        # pylint: disable=unused-variable
        _second_target_id = conn.execute(
            tables.target.insert().values(
                name="Protein Like Protease", short_name="PlPro"
            )
        ).inserted_primary_key[0]

        # Sample attributi
        conn.execute(
            tables.attributo.insert().values(
                [
                    {
                        "name": "crystal_buffer",
                        "description": "Crystal Buffer",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "micrograph_new",
                        "description": "Micrograph",
                        "json_schema": {"type": "string", "format": "image-path"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "shaking_time",
                        "description": "Shaking Time",
                        "json_schema": {"type": "string", "format": "duration"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "avg_crystal_size",
                        "description": "Average Crystal Size",
                        "json_schema": {
                            "type": "number",
                            "minimum": 0,
                            "suffix": "μm",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "crystallization_temperature",
                        "description": "Crystallization Temperature",
                        "json_schema": {
                            "type": "number",
                            "suffix": "°C",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "shaking_strength",
                        "description": "Shaking Strength",
                        "json_schema": {
                            "type": "number",
                            "suffix": "RPM",
                            "minimum": 0,
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "protein_concentration",
                        "description": "Protein Concentration",
                        "json_schema": {
                            "type": "number",
                            "suffix": "mg/mL",
                            "minimum": 0,
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "comment",
                        "description": "Comment",
                        "json_schema": {
                            "type": "string",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "crystal_settlement_volume",
                        "description": "Crystal Settlement Volume",
                        "json_schema": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 100,
                            "suffix": "%",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "seed_stock_used",
                        "description": "Seed Stock Used",
                        "json_schema": {
                            "type": "string",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "plate_origin",
                        "description": "Plate Origin",
                        "json_schema": {
                            "type": "string",
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "creator",
                        "description": "Creator",
                        "json_schema": {"type": "string", "format": "user-name"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "crystallization_method",
                        "description": "Crystallization Method",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "incubation_time",
                        "description": "Incubation Time",
                        "json_schema": {"type": "string", "format": "date-time"},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "crystal_shape",
                        "description": "Crystal Shape",
                        "json_schema": {
                            "type": "array",
                            "items": {"type": "number", "suffix": "nm", "minimum": 0},
                            "minItems": 3,
                            "maxItems": 3,
                        },
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                    {
                        "name": "filters",
                        "description": "Filters",
                        "json_schema": {"type": "array", "items": {"type": "string"}},
                        "associated_table": AssociatedTable.SAMPLE,
                    },
                ]
            )
        )

        # Create samples
        sample_ids: List[int] = []
        for i in range(10):
            sample_ids.append(
                conn.execute(
                    tables.sample.insert().values(
                        name=f"mpro {i}",
                        target_id=first_target_id,
                        modified=datetime.datetime.utcnow(),
                        attributi={
                            MANUAL_SOURCE_NAME: {
                                "crystal_buffer": "foo crystal buffer bar",
                                "shaking_time": "P2D",
                                "created": datetime.datetime.utcnow().isoformat(),
                            }
                        },
                    )
                ).inserted_primary_key[0]
            )

        conn.execute(
            tables.sample.insert().values(
                target_id=first_target_id,
                name="mpro unused",
                modified=datetime.datetime.utcnow(),
                attributi={
                    DB_SOURCE_NAME: {"created": datetime.datetime.utcnow().isoformat()}
                },
            )
        )

        # Create run properties
        conn.execute(
            tables.attributo.insert().values(
                [
                    {
                        "name": "repetition_rate",
                        "description": "Repetition Rate",
                        "json_schema": {
                            "type": "number",
                            "suffix": "MHz",
                            "minimumExclusive": 0,
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "status",
                        "description": "Status",
                        "json_schema": {
                            "type": "string",
                            "enum": ["running", "finished"],
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "xray_energy",
                        "description": "X-Ray energy",
                        "json_schema": {
                            "type": "number",
                            "suffix": "keV",
                            "minimum": 0,
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "pulse_energy",
                        "description": "Pulse energy",
                        "json_schema": {
                            "type": "number",
                            "suffix": "mJ",
                            "minimum": 0,
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "detector_gain",
                        "description": "Detector gain",
                        "json_schema": {
                            "type": "number",
                            "suffix": "dB",
                            "minimum": 0,
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "gain_mode",
                        "description": "Detector gain mode",
                        "json_schema": {
                            "type": "string",
                            "enum": [
                                "fixed high",
                                "fixed medium",
                                "fixed low",
                                "adaptive",
                            ],
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "hit_rate",
                        "description": "Hit Rate",
                        "json_schema": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 100,
                            "suffix": "%",
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "first_train",
                        "description": "First Train",
                        "json_schema": {"type": "integer"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "last_train",
                        "description": "Last Train",
                        "json_schema": {"type": "integer"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "injector_position_z",
                        "description": "Injector Position Z",
                        "json_schema": {"type": "number", "suffix": "mm"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "injector_flow_rate",
                        "description": "Injector Flow Rate",
                        "json_schema": {"type": "number", "suffix": "uL/min"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "sample_delivery_rate",
                        "description": "Sample Delivery Rate",
                        "json_schema": {"type": "number", "suffix": "uL/min"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "detector_darks",
                        "description": "Detector Darks",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "injector_valve_config",
                        "description": "Injector Valve Config",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "injector_image",
                        "description": "Injector Image",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "bunch_pattern",
                        "description": "Bunch Pattern",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "detector_quadrant_positions",
                        "description": "Detector Quadrant Positions",
                        "json_schema": {"type": "string"},
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "transmission",
                        "description": "Transmission",
                        "json_schema": {
                            "type": "number",
                            "minimum": 0,
                            "maximum": 100,
                            "suffix": "%",
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "tags",
                        "description": "Tags",
                        "json_schema": {
                            "type": "array",
                            "items": {
                                "type": "string",
                                "minLength": 1,
                                "format": "tag",
                            },
                        },
                        "associated_table": AssociatedTable.RUN,
                    },
                    {
                        "name": "started",
                        "description": "Started",
                        "json_schema": {"type": "string", "format": "date-time"},
                        "associated_table": AssociatedTable.RUN,
                    },
                ]
            )
        )

        # Create runs
        base_date = datetime.datetime.utcnow()
        # To always get the same sample data, yet somewhat random values
        seed(1337)
        run_ids: List[int] = []
        current_train = 0
        # for _ in range(10000):
        for _ in range(100):
            train_count = randint(600, 3000)
            run_id = conn.execute(
                tables.run.insert().values(
                    proposal_id=proposal_id,
                    modified=datetime.datetime.utcnow(),
                    sample_id=choice(sample_ids),
                    attributi={
                        "karabo": {
                            "started": (
                                base_date + datetime.timedelta(0, randint(10, 200))
                            ).isoformat(),
                            "status": "running",
                            "repetition_rate": randrange(0, 20),
                            "hit_rate": random(),
                            "injector_position_z": randrange(0, 100),
                            "first_train": current_train + 1,
                            "last_train": current_train + train_count,
                        }
                    },
                )
            ).inserted_primary_key[0]

            current_train += train_count

            run_ids.append(run_id)

            for _ in range(randrange(0, 10)):
                conn.execute(
                    tables.run_comment.insert().values(
                        run_id=run_id,
                        author="testauthor",
                        comment_text="foooooo",
                        created=datetime.datetime.utcnow(),
                    )
                )

        # Insert analysis results
        # For each run, add 1 to 2 data sources
        # For each data source, generate 1 to 2 peak search result, hit finding parameters and hit finding results
        # For each hit finding result, generate 1 to 2 indexing parameters, integration parameters  and indexing results
        # For indexing result, generate 1 or 2 merge results

        possible_tags: Final = [
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "noice",
            "newtry",
            "foo",
            "othertag",
            "goodweather",
            "spring",
            "more_kev",
            "pixhigher",
        ]

        for run_id in run_ids:
            for _ in range(choice([1, 2])):
                number_of_frames = randint(0, 1000)
                data_source_id = conn.execute(
                    tables.data_source.insert().values(
                        run_id=run_id,
                        number_of_frames=number_of_frames,
                        tag=choice(possible_tags),
                        source=[
                            "/var/log/foo/bar/bux/quuux/baaaaaaaaar/hoooo.h5"
                            for _ in range(40)
                        ],
                    )
                ).inserted_primary_key[0]

                for _ in range(choice([0, 1, 2])):
                    peak_search_parameters_id = conn.execute(
                        tables.peak_search_parameters.insert().values(
                            data_source_id=data_source_id,
                            method="dummy-method",
                            software="dummy-software",
                            command_line="command-line",
                            tag=choice(possible_tags),
                        )
                    ).inserted_primary_key[0]

                    hit_finding_parameters_id = conn.execute(
                        tables.hit_finding_parameters.insert().values(
                            min_peaks=int(uniform(10, 100)),
                            tag=choice(possible_tags),
                        )
                    ).inserted_primary_key[0]

                    hit_finding_results_id = conn.execute(
                        tables.hit_finding_results.insert().values(
                            peak_search_parameters_id=peak_search_parameters_id,
                            hit_finding_parameters_id=hit_finding_parameters_id,
                            number_of_hits=int(uniform(0, number_of_frames)),
                            hit_rate=random() * 100,
                            result_filename="/tmp/result",
                            tag=choice(possible_tags),
                        )
                    ).inserted_primary_key[0]

                    for _ in range(choice([0, 1, 2])):
                        indexing_parameters_id = conn.execute(
                            tables.indexing_parameters.insert().values(
                                hit_finding_results_id=hit_finding_results_id,
                                software="dummy-software",
                                command_line="",
                                parameters={},
                                tag=choice(possible_tags),
                            )
                        ).inserted_primary_key[0]

                        integration_parameters_id = conn.execute(
                            tables.integration_parameters.insert().values(
                                tag=choice(possible_tags)
                            )
                        ).inserted_primary_key[0]

                        indexing_results_id = conn.execute(
                            tables.indexing_results.insert().values(
                                indexing_parameters_id=indexing_parameters_id,
                                integration_parameters_id=integration_parameters_id,
                                num_indexed=int(uniform(0, number_of_frames)),
                                num_crystals=1,
                                tag=choice(possible_tags),
                            )
                        ).inserted_primary_key[0]

                        for _ in range(choice([1, 2])):
                            merge_parameters_id = conn.execute(
                                tables.merge_parameters.insert().values(
                                    software="dummy-software",
                                    parameters={},
                                    command_line="",
                                    tag=choice(possible_tags),
                                )
                            ).inserted_primary_key[0]

                            merge_results_id = conn.execute(
                                tables.merge_results.insert().values(
                                    merge_parameters_id=merge_parameters_id,
                                    rsplit=random(),
                                    cc_half=random(),
                                )
                            ).inserted_primary_key[0]

                            conn.execute(
                                tables.merge_has_indexing.insert().values(
                                    merge_results_id=merge_results_id,
                                    indexing_results_id=indexing_results_id,
                                )
                            )

    logger.info("Done")
