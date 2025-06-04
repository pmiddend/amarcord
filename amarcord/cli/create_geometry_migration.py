from dataclasses import dataclass
import datetime
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text
import sqlalchemy as sa
import sys

import structlog

logger = structlog.stdlib.get_logger(__name__)

engine = create_engine(sys.argv[1])


@dataclass(frozen=True)
class ErrorMessage:
    message: str


BeamtimeId = int
GeometryHash = str


def geometry_hash(fp: Path) -> ErrorMessage | GeometryHash:
    try:
        with fp.open("rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except Exception as e:
        return ErrorMessage(str(e))


@dataclass
class Geometry:
    filename: str
    indexing_parameters: list[int]
    indexing_results_generated_geometries: list[int]
    content: str


GEOMETRY_TABLE = sa.sql.table(
    "Geometry",
    sa.column("id", sa.Integer),
    sa.column("beamtime_id", sa.Integer),
    sa.column("content", sa.Text),
    sa.column("hash", sa.String(length=64)),
    sa.column("name", sa.String(length=255)),
    sa.column("created", sa.Datetime),
)

INDEXING_PARAMETERS_TABLE = sa.sql.table(
    "IndexingParameters",
    sa.column("id", sa.Integer),
    sa.column("geometry_id", sa.Integer),
)

INDEXING_RESULTS_TABLE = sa.sql.table(
    "IndexingResults",
    sa.column("id", sa.Integer),
    sa.column("generated_geometry_id", sa.Integer),
)


def generate_name(existing_names: set[str], path: Path) -> str:
    if path.name not in existing_names:
        return path.name
    return generate_name(existing_names, path.parent / f"{path.stem}_new{path.suffix}")


with engine.connect() as conn:
    result = conn.execute(
        text(
            "SELECT I.id, I.geometry_file, I.generated_geometry_file, I.indexing_parameters_id FROM IndexingResult I"
        )
    )

    geometries: dict[tuple[BeamtimeId, GeometryHash], Geometry] = {}
    existing_names: dict[BeamtimeId, set[str]] = {}
    for row in result:
        ir_logger = logger.bind(ir_id=row.id)
        beamtime_id = (
            conn.execute(
                text("SELECT Run.beamtime_id FROM Run WHERE id = :r"), {"r": row.run_id}
            )
            .scalar_one()
            .beamtime_id
        )

        if row.geometry_file is None:
            ir_logger.warning("does not have a geometry set, skipping")
            continue

        hash = geometry_hash(Path(row.geometry_file))

        if isinstance(hash, ErrorMessage):
            ir_logger.warning(
                f"couldn't determine geometry hash {row.geometry_file}: {hash.message}"
            )
            continue

        existing_geometry = geometries.get((beamtime_id, hash))

        geometry_name_in_db = generate_name(
            existing_names.get(beamtime_id, set()),
            row.geometry_file,
        )
        if beamtime_id not in existing_names:
            existing_names[beamtime_id] = set()
        existing_names[beamtime_id].add(geometry_name_in_db)

        if existing_geometry is not None:
            existing_geometry.indexing_parameters.append(row.indexing_parameters_id)
        else:
            with Path(row.geometry_file).open("r", encoding="utf-8") as f:
                content = f.read()
            geometries[(beamtime_id, hash)] = Geometry(
                indexing_parameters=[row.indexing_parameters_id],
                indexing_results_generated_geometries=[],
                filename=geometry_name_in_db,
                content=content,
            )

        if row.generated_geometry_file is not None:
            generated_hash = geometry_hash(Path(row.geometry_file))

            if isinstance(generated_hash, ErrorMessage):
                ir_logger.warning(
                    f"couldn't determine generated geometry hash {row.generated_geometry_file}: {generated_hash.message}"
                )
                continue

            existing_generated_geometry = geometries.get((beamtime_id, generated_hash))
            generated_geometry_name_in_db = generate_name(
                existing_names.get(beamtime_id, set()),
                row.generated_geometry_file,
            )
            if beamtime_id not in existing_names:
                existing_names[beamtime_id] = set()
            existing_names[beamtime_id].add(generated_geometry_name_in_db)

            if existing_generated_geometry:
                existing_generated_geometry.indexing_results_generated_geometries.append(
                    row.id
                )
            else:
                with Path(row.generated_geometry_file).open("r", encoding="utf-8") as f:
                    content = f.read()
                geometries[(beamtime_id, generated_hash)] = Geometry(
                    indexing_parameters=[],
                    indexing_results_generated_geometries=[row.id],
                    filename=generated_geometry_name_in_db,
                    content=content,
                )

    for (beamtime_id, hash), geometry in geometries.items():
        insert_result = conn.execute(
            GEOMETRY_TABLE.insert().values(
                {
                    "beamtime_id": beamtime_id,
                    "content": geometry,
                    "hash": hash,
                    "name": geometry.filename,
                    "created": datetime.datetime.now(datetime.timezone.utc),
                }
            )
        )  # type: ignore

        # For some reason, at least for sqlite, this doesn't work. It returns an empty tuple.
        # online_parameters_id: int = prior_parameters_insert.inserted_primary_key[0]
        # print(online_parameters_id)
        # This works, but only for certain backends. But our backends are among it, so should be fine.
        new_geometry_id = insert_result.lastrowid

        conn.execute(
            INDEXING_PARAMETERS_TABLE.update()
            .where(INDEXING_PARAMETERS_TABLE.c.id.in_(geometry.indexing_parameters))
            .values({"geometry_id": new_geometry_id})
        )

        conn.execute(
            INDEXING_RESULTS_TABLE.update()
            .where(
                INDEXING_RESULTS_TABLE.c.id.in_(
                    geometry.indexing_results_generated_geometries
                )
            )
            .values({"generated_geometry_id": new_geometry_id})
        )

        logger.info(f"beamtime: {beamtime_id}, geometry hash: {hash}")
        logger.info(
            "indexing parameters: "
            + ", ".join(str(s) for s in geometry.indexing_parameters)
        )
        logger.info(
            "generated results: "
            + ", ".join(str(s) for s in geometry.indexing_results_generated_geometries)
        )
