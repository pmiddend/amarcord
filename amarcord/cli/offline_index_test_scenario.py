import asyncio
import datetime

import structlog
from sqlalchemy.ext.asyncio import create_async_engine
from tap import Tap

from amarcord.cli.crystfel_index import sha256_bytes
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.constants import CELL_DESCRIPTION_ATTRIBUTO
from amarcord.db.constants import POINT_GROUP_ATTRIBUTO
from amarcord.db.geometry_type import GeometryType
from amarcord.db.orm import Attributo
from amarcord.db.orm import Beamtime
from amarcord.db.orm import Chemical
from amarcord.db.orm import ChemicalHasAttributoValue
from amarcord.db.orm import DataSet
from amarcord.db.orm import DataSetHasAttributoValue
from amarcord.db.orm import ExperimentHasAttributo
from amarcord.db.orm import ExperimentType
from amarcord.db.orm import Geometry
from amarcord.db.orm import Run
from amarcord.db.orm import RunHasAttributoValue
from amarcord.db.orm import RunHasFiles
from amarcord.db.orm_utils import migrate
from amarcord.db.run_external_id import RunExternalId
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    db_connection_url: (
        str  # Connection URL for the database (e.g. pymysql+mysql://foo/bar)
    )
    h5_glob: str


async def _main(args: Arguments) -> None:
    engine = create_async_engine(args.db_connection_url)
    logger.info("migrating DB to latest version")
    await migrate(engine)

    async with get_orm_sessionmaker_with_url(args.db_connection_url)() as session:
        beamtime = Beamtime(
            external_id="11010000",
            proposal="",
            beamline="",
            title="test beamtime",
            comment="",
            start=datetime.datetime.now(),
            end=datetime.datetime.now(),
            analysis_output_path="/tmp/analysis-output",  # noqa: S108
        )
        session.add(beamtime)
        await session.flush()
        logger.info(f"created beam time, ID {beamtime.id}")
        run_chemical_attributo = Attributo(
            beamtime_id=beamtime.id,
            name="channel_1_chemical_id",
            description="",
            group="manual",
            associated_table=AssociatedTable.RUN,
            json_schema={"type": "integer", "format": "chemical-id"},
        )
        cell_description_attributo = Attributo(
            beamtime_id=beamtime.id,
            name=CELL_DESCRIPTION_ATTRIBUTO,
            description="",
            group="manual",
            associated_table=AssociatedTable.CHEMICAL,
            json_schema={"type": "string"},
        )
        point_group_attributo = Attributo(
            beamtime_id=beamtime.id,
            name=POINT_GROUP_ATTRIBUTO,
            description="",
            group="manual",
            associated_table=AssociatedTable.CHEMICAL,
            json_schema={"type": "string"},
        )
        session.add(cell_description_attributo)
        session.add(point_group_attributo)
        session.add(run_chemical_attributo)
        await session.flush()
        logger.info("created necessary attributi")
        experiment_type = ExperimentType(
            beamtime_id=beamtime.id,
            name="test experiment type",
            attributi=[
                ExperimentHasAttributo(
                    attributo_id=run_chemical_attributo.id,
                    chemical_role=ChemicalType.CRYSTAL,
                ),
            ],
        )
        session.add(experiment_type)
        logger.info(f"created experiment type with id {experiment_type.id}")
        await session.flush()
        chemical = Chemical(
            beamtime_id=beamtime.id,
            name="lyso",
            responsible_person="Philipp",
            modified=datetime.datetime.now(),
            type=ChemicalType.CRYSTAL,
            attributo_values=[
                ChemicalHasAttributoValue(
                    attributo_id=point_group_attributo.id,
                    integer_value=None,
                    float_value=None,
                    string_value="mmm",
                    bool_value=None,
                    datetime_value=None,
                    list_value=None,
                ),
            ],
        )
        session.add(chemical)
        await session.flush()
        logger.info(f"created chemical with id {chemical.id}")
        run_1 = Run(
            external_id=RunExternalId(1),
            beamtime_id=beamtime.id,
            modified=datetime.datetime.now(),
            started=datetime.datetime.now(),
            stopped=datetime.datetime.now(),
            experiment_type_id=experiment_type.id,
            attributo_values=[
                RunHasAttributoValue(
                    attributo_id=run_chemical_attributo.id,
                    integer_value=None,
                    float_value=None,
                    string_value=None,
                    bool_value=None,
                    datetime_value=None,
                    list_value=None,
                    chemical_value=chemical.id,
                ),
            ],
            files=[RunHasFiles(glob=args.h5_glob, source="h5")],
        )
        session.add(run_1)
        # run_2 = Run(
        #     external_id=RunExternalId(2),
        #     beamtime_id=beamtime.id,
        #     modified=datetime.datetime.now(),
        #     started=datetime.datetime.now(),
        #     stopped=datetime.datetime.now(),
        #     experiment_type_id=experiment_type.id,
        #     attributo_values=[
        #         RunHasAttributoValue(
        #             attributo_id=run_chemical_attributo.id,
        #             integer_value=None,
        #             float_value=None,
        #             string_value=None,
        #             bool_value=None,
        #             datetime_value=None,
        #             list_value=None,
        #             chemical_value=chemical.id,
        #         ),
        #     ],
        #     files=[
        #         RunHasFiles(
        #             glob="/asap3/petra3/gpfs/p11/2024/data/11017935/raw/nexus-files/134/haspp11e16m-100g/134*"
        #         )
        #     ],
        # )
        # session.add(run_2)
        await session.flush()
        # logger.info(f"created internal run ids {run_1.id}, {run_2.id}")

        data_set = DataSet(
            experiment_type_id=experiment_type.id,
            attributo_values=[
                DataSetHasAttributoValue(
                    attributo_id=run_chemical_attributo.id,
                    integer_value=None,
                    float_value=None,
                    string_value=None,
                    bool_value=None,
                    datetime_value=None,
                    list_value=None,
                    chemical_value=chemical.id,
                ),
            ],
        )
        session.add(data_set)

        geometry_content = """
photon_energy = 12000 eV
clen = 0.1986
bandwidth = 1.000000e-08
coffset = 0.000000
res = 13333.300000
data = /entry/data/data
flag_morethan = 65534
adu_per_photon = 1.000000
dim0 = %
dim1 = ss
dim2 = fs

panel0/min_fs = 0
panel0/max_fs = 4147
panel0/min_ss = 0
panel0/max_ss = 4361
panel0/corner_x = 2139.097467
panel0/corner_y = 2182.737834
panel0/fs = -1.000000x +0.000000y +0.000000z
panel0/ss = 0.000000x -1.000000y +0.000000z

group_all = panel0
            """
        geometry = Geometry(
            beamtime_id=beamtime.id,
            content=geometry_content,
            name="first geometry",
            hash=sha256_bytes(geometry_content.encode("utf-8")),
            created=datetime.datetime.now(datetime.timezone.utc),
            geometry_type=GeometryType.CRYSTFEL_STRING,
        )
        session.add(geometry)
        # indexing_params_with_cell = IndexingParameters(
        #     is_online=False,
        #     cell_description="orthorhombic P ? (51.00 89.00 94.00) (90 90 90)",
        # )
        # indexing_params_no_cell = IndexingParameters(
        #     is_online=False,
        #     cell_description=None,
        # )
        # session.add(indexing_params_with_cell)
        # session.add(indexing_params_no_cell)
        # await session.flush()
        # logger.info(f"indexing params with cell {indexing_params_with_cell.id}")
        # logger.info(f"indexing params without cell {indexing_params_no_cell.id}")
        # indexing_result_with_cell_run_1 = IndexingResult(
        #     created=datetime.datetime.now(),
        #     run_id=run_1.id,
        #     stream_file="/gpfs/cfel/user/pmidden/offline-indexing-test-output/output-1.stream",
        #     frames=0,
        #     hits=0,
        #     indexed_frames=0,
        #     detector_shift_x_mm=0,
        #     detector_shift_y_mm=0,
        #     a_histogram_file_id=None,
        #     b_histogram_file_id=None,
        #     c_histogram_file_id=None,
        #     alpha_histogram_file_id=None,
        #     beta_histogram_file_id=None,
        #     gamma_histogram_file_id=None,
        #     job_id=None,
        #     job_status=DBJobStatus.QUEUED,
        #     job_error=None,
        #     job_started=None,
        #     job_stopped=None,
        #     job_latest_log="",
        #     indexing_parameters_id=indexing_params_with_cell.id,
        # )
        # session.add(indexing_result_with_cell_run_1)
        # indexing_result_with_cell_run_2 = IndexingResult(
        #     created=datetime.datetime.now(),
        #     run_id=run_2.id,
        #     stream_file="/gpfs/cfel/user/pmidden/offline-indexing-test-output/output-2.stream",
        #     frames=0,
        #     hits=0,
        #     indexed_frames=0,
        #     detector_shift_x_mm=0,
        #     detector_shift_y_mm=0,
        #     a_histogram_file_id=None,
        #     b_histogram_file_id=None,
        #     c_histogram_file_id=None,
        #     alpha_histogram_file_id=None,
        #     beta_histogram_file_id=None,
        #     gamma_histogram_file_id=None,
        #     job_id=None,
        #     job_status=DBJobStatus.QUEUED,
        #     job_error=None,
        #     job_latest_log="",
        #     job_started=None,
        #     job_stopped=None,
        #     indexing_parameters_id=indexing_params_with_cell.id,
        # )
        # session.add(indexing_result_with_cell_run_2)
        # indexing_result_no_cell_run_1 = IndexingResult(
        #     created=datetime.datetime.now(),
        #     run_id=run_1.id,
        #     stream_file="/gpfs/cfel/user/pmidden/offline-indexing-test-output/output-1.stream",
        #     frames=0,
        #     hits=0,
        #     indexed_frames=0,
        #     detector_shift_x_mm=0,
        #     detector_shift_y_mm=0,
        #     a_histogram_file_id=None,
        #     b_histogram_file_id=None,
        #     c_histogram_file_id=None,
        #     alpha_histogram_file_id=None,
        #     beta_histogram_file_id=None,
        #     gamma_histogram_file_id=None,
        #     job_id=None,
        #     job_status=DBJobStatus.QUEUED,
        #     job_error=None,
        #     job_latest_log="",
        #     job_started=None,
        #     job_stopped=None,
        #     indexing_parameters_id=indexing_params_no_cell.id,
        # )
        # session.add(indexing_result_no_cell_run_1)
        # indexing_result_no_cell_run_2 = IndexingResult(
        #     created=datetime.datetime.now(),
        #     run_id=run_2.id,
        #     stream_file="/gpfs/cfel/user/pmidden/offline-indexing-test-output/output-2.stream",
        #     frames=0,
        #     hits=0,
        #     indexed_frames=0,
        #     detector_shift_x_mm=0,
        #     detector_shift_y_mm=0,
        #     a_histogram_file_id=None,
        #     b_histogram_file_id=None,
        #     c_histogram_file_id=None,
        #     alpha_histogram_file_id=None,
        #     beta_histogram_file_id=None,
        #     gamma_histogram_file_id=None,
        #     job_id=None,
        #     job_status=DBJobStatus.QUEUED,
        #     job_error=None,
        #     job_started=None,
        #     job_stopped=None,
        #     job_latest_log="",
        #     indexing_parameters_id=indexing_params_no_cell.id,
        # )
        # session.add(indexing_result_no_cell_run_2)
        await session.commit()


def main() -> None:
    asyncio.run(_main(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
