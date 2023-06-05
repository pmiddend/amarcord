import datetime
from pathlib import Path
from typing import Final

import pytest
import structlog

from amarcord.amici.crystfel.indexing_daemon import CrystFELOnlineConfig
from amarcord.amici.crystfel.indexing_daemon import start_indexing_job
from amarcord.amici.crystfel.util import ATTRIBUTO_PROTEIN
from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.amici.crystfel.util import write_cell_file
from amarcord.amici.workload_manager.dummy_workload_manager import DummyWorkloadManager
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.tables import create_tables_from_metadata

logger = structlog.stdlib.get_logger(__name__)

_VALID_CELL_DESCRIPTION = CrystFELCellFile(
    lattice_type="tetragonal",
    centering="P",
    unique_axis="c",
    a=79.2,
    b=79.2,
    c=38.0,
    alpha=90.0,
    beta=90.0,
    gamma=90.00,
)
_VALID_POINT_GROUP = "4/mmm"


@pytest.mark.parametrize(
    "input_string, cell_file",
    [
        (
            "monoclinic P c (3.4 5.6 7.8) (40.0 50.0 60.0)",
            CrystFELCellFile(
                lattice_type="monoclinic",
                centering="P",
                unique_axis="c",
                a=3.4,
                b=5.6,
                c=7.8,
                alpha=40.0,
                beta=50.0,
                gamma=60.0,
            ),
        ),
        (
            "monoclinic P ? (3.4 5.6 7.8) (40.0 50.0 60.0)",
            CrystFELCellFile(
                lattice_type="monoclinic",
                centering="P",
                unique_axis=None,
                a=3.4,
                b=5.6,
                c=7.8,
                alpha=40.0,
                beta=50.0,
                gamma=60.0,
            ),
        ),
        ("monoclini P ? (3.4 5.6 7.8) (40.0 50.0 60.0)", None),
    ],
)
def test_parse_cell_description(
    input_string: str, cell_file: None | CrystFELCellFile
) -> None:
    assert parse_cell_description(input_string) == cell_file


# pylint: disable=unused-argument
def test_write_cell_file(tmp_path: Path) -> None:
    output = tmp_path / Path("output.cell")
    write_cell_file(
        CrystFELCellFile(
            lattice_type="monoclinic",
            centering="P",
            unique_axis="c",
            a=3.4,
            b=5.6,
            c=7.8,
            alpha=40.0,
            beta=50.0,
            gamma=60.0,
        ),
        output,
    )
    with output.open("r") as f:
        contents = f.readlines()
        # Just a smoke test
        assert contents[0] == "CrystFEL unit cell file version 1.0\n"


_TEST_DB_URL: Final = "sqlite+aiosqlite://"


async def _get_db(use_sqlalchemy_default_json_serializer: bool = False) -> AsyncDB:
    context = AsyncDBContext(
        _TEST_DB_URL,
        use_sqlalchemy_default_json_serializer=use_sqlalchemy_default_json_serializer,
    )
    db = AsyncDB(context, create_tables_from_metadata(context.metadata))
    await db.migrate()
    return db


async def _create_indexing_scenario(db: AsyncDB, cell_description: None | str) -> int:
    async with db.begin() as conn:
        await db.create_attributo(
            conn,
            ATTRIBUTO_PROTEIN,
            "",
            "manual",
            AssociatedTable.RUN,
            AttributoTypeChemical(),
        )
        await db.create_attributo(
            conn,
            "cell description",
            "",
            "manual",
            AssociatedTable.CHEMICAL,
            AttributoTypeString(),
        )
        attributi = await db.retrieve_attributi(conn, None)
        chemical_id = await db.create_chemical(
            conn=conn,
            name="lyso",
            responsible_person="Rosalind Franklin",
            type_=ChemicalType.CRYSTAL,
            attributi=AttributiMap.from_types_and_raw(
                attributi,
                [],
                {AttributoId("cell description"): cell_description}
                if cell_description is not None
                else {},
            ),
        )
        await db.create_run(
            conn,
            run_id=1,
            attributi=attributi,
            attributi_map=AttributiMap.from_types_and_raw(
                attributi, [chemical_id], {ATTRIBUTO_PROTEIN: chemical_id}
            ),
            keep_manual_attributes_from_previous_run=True,
        )
        return chemical_id


async def test_start_indexing_job_valid_cell_file(tmp_path: Path) -> None:
    db = await _get_db()

    protein_id = await _create_indexing_scenario(
        db, coparse_cell_description(_VALID_CELL_DESCRIPTION)
    )

    async with db.begin() as conn:
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                cell_description=_VALID_CELL_DESCRIPTION,
                point_group=_VALID_POINT_GROUP,
                chemical_id=protein_id,
                run_id=1,
                frames=0,
                hits=0,
                not_indexed_frames=0,
                runtime_status=None,
            ),
        )
        indexing_results = await db.retrieve_indexing_results(conn)
        assert indexing_results
        indexing_result = indexing_results[0]

    base_dir = tmp_path / Path("output")
    base_dir.mkdir(parents=True)
    cell_file_dir = base_dir / "cell-files"
    cell_file_dir.mkdir(parents=True)
    crystfel_path = base_dir / "crystfel"
    workload_manager = DummyWorkloadManager()
    workload_manager.job_start_results.append(
        JobStartResult(job_id=1, metadata=JobMetadata({}))
    )
    ir = await start_indexing_job(
        workload_manager,
        CrystFELOnlineConfig(
            output_base_directory=base_dir,
            crystfel_path=crystfel_path,
            api_url="http://localhost",
            dummy_h5_input=None,
        ),
        indexing_result,
    )
    assert isinstance(ir, DBIndexingResultRunning)
    assert len(workload_manager.job_starts) == 1
    assert workload_manager.job_starts[0].working_directory == base_dir


async def test_start_indexing_job_no_cell_file(tmp_path: Path) -> None:
    db = await _get_db()

    protein_id = await _create_indexing_scenario(db, cell_description=None)

    async with db.begin() as conn:
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                cell_description=None,
                point_group=None,
                chemical_id=protein_id,
                run_id=1,
                frames=0,
                hits=0,
                not_indexed_frames=0,
                runtime_status=None,
            ),
        )
        indexing_results = await db.retrieve_indexing_results(conn)
        assert indexing_results
        indexing_result = indexing_results[0]

    base_dir = tmp_path / Path("output")
    base_dir.mkdir(parents=True)
    cell_file_dir = base_dir / "cell-files"
    cell_file_dir.mkdir(parents=True)
    crystfel_path = base_dir / "crystfel"
    workload_manager = DummyWorkloadManager()
    workload_manager.job_start_results.append(
        JobStartResult(job_id=1, metadata=JobMetadata({}))
    )
    ir = await start_indexing_job(
        workload_manager,
        CrystFELOnlineConfig(
            output_base_directory=base_dir,
            crystfel_path=crystfel_path,
            api_url="http://localhost",
            dummy_h5_input=None,
        ),
        indexing_result,
    )
    assert isinstance(ir, DBIndexingResultRunning)
    assert not [x for x in cell_file_dir.iterdir() if x.name.endswith(".cell")]
    assert len(workload_manager.job_starts) == 1
    assert workload_manager.job_starts[0].working_directory == base_dir


async def test_start_indexing_job_start_error(tmp_path: Path) -> None:
    db = await _get_db()

    protein_id = await _create_indexing_scenario(
        db, coparse_cell_description(_VALID_CELL_DESCRIPTION)
    )

    async with db.begin() as conn:
        await db.create_indexing_result(
            conn,
            DBIndexingResultInput(
                created=datetime.datetime.utcnow(),
                cell_description=_VALID_CELL_DESCRIPTION,
                point_group=_VALID_POINT_GROUP,
                chemical_id=protein_id,
                run_id=1,
                frames=0,
                hits=0,
                not_indexed_frames=0,
                runtime_status=None,
            ),
        )
        indexing_results = await db.retrieve_indexing_results(conn)
        assert indexing_results
        indexing_result = indexing_results[0]

    base_dir = tmp_path / Path("output")
    base_dir.mkdir(parents=True)
    cell_file_dir = base_dir / "cell-files"
    cell_file_dir.mkdir(parents=True)
    crystfel_path = base_dir / "crystfel"
    workload_manager = DummyWorkloadManager()
    # None her signifies a job start error
    workload_manager.job_start_results.append(None)
    ir = await start_indexing_job(
        workload_manager,
        CrystFELOnlineConfig(
            output_base_directory=base_dir,
            crystfel_path=crystfel_path,
            api_url="http://localhost",
            dummy_h5_input=None,
        ),
        indexing_result,
    )
    assert workload_manager.job_starts
    assert isinstance(ir, DBIndexingResultDone)
    assert ir.job_error is not None
