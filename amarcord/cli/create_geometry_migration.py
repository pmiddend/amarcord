from dataclasses import dataclass
import hashlib
from pathlib import Path
from sqlalchemy import create_engine, text
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
    indexing_parameters: list[int]
    indexing_results_generated_geometries: list[int]


with engine.connect() as conn:
    result = conn.execute(
        text(
            "SELECT I.id, I.geometry_file, I.generated_geometry_file, I.indexing_parameters_id FROM IndexingResult I"
        )
    )

    geometries: dict[tuple[BeamtimeId, GeometryHash], Geometry] = {}
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

        if existing_geometry is not None:
            existing_geometry.indexing_parameters.append(row.indexing_parameters_id)
        else:
            geometries[(beamtime_id, hash)] = Geometry(
                indexing_parameters=[row.indexing_parameters_id],
                indexing_results_generated_geometries=[],
            )

        if row.generated_geometry_file is not None:
            generated_hash = geometry_hash(Path(row.geometry_file))

            if isinstance(generated_hash, ErrorMessage):
                ir_logger.warning(
                    f"couldn't determine generated geometry hash {row.generated_geometry_file}: {generated_hash.message}"
                )
                continue

            existing_generated_geometry = geometries.get((beamtime_id, generated_hash))

            if existing_generated_geometry:
                existing_generated_geometry.indexing_results_generated_geometries.append(
                    row.id
                )
            else:
                geometries[(beamtime_id, generated_hash)] = Geometry(
                    indexing_parameters=[],
                    indexing_results_generated_geometries=[row.id],
                )

    for (beamtime_id, hash), geometry in geometries.items():
        logger.info(f"beamtime: {beamtime_id}, geometry hash: {hash}")
        logger.info(
            "indexing parameters: "
            + ", ".join(str(s) for s in geometry.indexing_parameters)
        )
        logger.info(
            "generated results: "
            + ", ".join(str(s) for s in geometry.indexing_results_generated_geometries)
        )
