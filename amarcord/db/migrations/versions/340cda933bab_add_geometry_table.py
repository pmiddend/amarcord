"""add geometry table

Revision ID: 340cda933bab
Revises: bb5c96f181f3
Create Date: 2025-05-07 14:52:48.472891

"""

from dataclasses import dataclass
import datetime
import hashlib
from pathlib import Path
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "340cda933bab"
down_revision = "bb5c96f181f3"
branch_labels = None
depends_on = None


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
    sa.column("created", sa.DateTime),
)

INDEXING_PARAMETERS_TABLE = sa.sql.table(
    "IndexingParameters",
    sa.column("id", sa.Integer),
    sa.column("geometry_id", sa.Integer),
)

INDEXING_RESULTS_TABLE = sa.sql.table(
    "IndexingResult",
    sa.column("id", sa.Integer),
    sa.column("generated_geometry_id", sa.Integer),
)


def generate_name(existing_names: set[str], path: Path) -> str:
    if path.name not in existing_names:
        return path.name
    return generate_name(existing_names, path.parent / f"{path.stem}_new{path.suffix}")


def upgrade() -> None:
    op.create_table(
        "Geometry",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            sa.ForeignKey(
                "Beamtime.id", ondelete="cascade", name="geometry_has_beamtime_id_fk"
            ),
            nullable=False,
        ),
        sa.Column(
            "content",
            sa.Text,
            nullable=False,
        ),
        sa.Column(
            "hash",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "name",
            sa.String(length=255),
            nullable=False,
        ),
        sa.Column(
            "created",
            sa.DateTime,
            nullable=False,
        ),
    )
    op.create_table(
        "GeometryReferencesAttributo",
        sa.Column(
            "geometry_id",
            sa.Integer,
            sa.ForeignKey("Geometry.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            sa.ForeignKey("Attributo.id", ondelete="cascade"),
            nullable=False,
        ),
    )
    op.create_table(
        "GeometryTemplateReplacement",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "indexing_result_id",
            sa.Integer,
            sa.ForeignKey("IndexingResult.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            sa.ForeignKey("Attributo.id", ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("replacement", sa.String(length=255), nullable=False),
    )
    with op.batch_alter_table("IndexingParameters") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "geometry_id",
                sa.Integer,
                sa.ForeignKey(
                    "Geometry.id",
                    ondelete="cascade",
                    name="indexing_parameters_geometry_fk",
                ),
                nullable=True,
            ),
        )
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        batch_op.add_column(
            sa.Column(
                "generated_geometry_id",
                sa.Integer,
                sa.ForeignKey(
                    "Geometry.id",
                    ondelete="cascade",
                    name="indexing_result_generated_geometry_fk",
                ),
                nullable=True,
            ),
        )
    conn = op.get_bind()
    result = conn.execute(
        sa.text(
            "SELECT I.id, I.run_id, I.geometry_file, I.generated_geometry_file, I.indexing_parameters_id FROM IndexingResult I"
        )
    )

    geometries: dict[tuple[BeamtimeId, GeometryHash], Geometry] = {}
    existing_names: dict[BeamtimeId, set[str]] = {}
    for row in result:
        beamtime_id = conn.execute(
            sa.text("SELECT Run.beamtime_id FROM Run WHERE id = :r"),
            {"r": row.run_id},
        ).scalar_one()

        if not row.geometry_file:
            print("does not have a geometry set, skipping")
            continue

        hash = geometry_hash(Path(row.geometry_file))

        if isinstance(hash, ErrorMessage):
            print(
                f"couldn't determine geometry hash {row.geometry_file}: {hash.message}"
            )
            continue

        existing_geometry = geometries.get((beamtime_id, hash))

        geometry_name_in_db = generate_name(
            existing_names.get(beamtime_id, set()),
            Path(row.geometry_file),
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
                print(
                    f"couldn't determine generated geometry hash {row.generated_geometry_file}: {generated_hash.message}"
                )
                continue

            existing_generated_geometry = geometries.get((beamtime_id, generated_hash))
            generated_geometry_name_in_db = generate_name(
                existing_names.get(beamtime_id, set()),
                Path(row.generated_geometry_file),
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
                    "content": geometry.content,
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

        print(f"beamtime: {beamtime_id}, geometry hash: {hash}")
        print(
            "indexing parameters: "
            + ", ".join(str(s) for s in geometry.indexing_parameters)
        )
        print(
            "generated results: "
            + ", ".join(str(s) for s in geometry.indexing_results_generated_geometries)
        )

    with op.batch_alter_table("IndexingParameters") as batch_op:  # type: ignore
        batch_op.drop_column("geometry_file")
    with op.batch_alter_table("IndexingResult") as batch_op:  # type: ignore
        # Remove geometry ID column
        batch_op.drop_column("geometry_file")
        batch_op.drop_column("geometry_hash")
        batch_op.drop_column("generated_geometry_file")


def downgrade() -> None:
    pass
