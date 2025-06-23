from pathlib import Path

from amarcord.db.attributi import local_int_to_utc_datetime
from amarcord.web.json_models import JsonBeamtimeOutput


def determine_output_directory(
    beamtime: JsonBeamtimeOutput,
    additional_replacements: dict[str, str],
) -> Path:
    # This is a gratuitous selection of beamtime metadata. Please add necessary fields if you need them.
    job_base_directory_str = (
        beamtime.analysis_output_path.replace(
            "{beamtime.external_id}",
            beamtime.external_id,
        )
        .replace(
            "{beamtime.year}",
            str(local_int_to_utc_datetime(beamtime.start_local).year),
        )
        .replace("{beamtime.beamline}", beamtime.beamline)
        .replace("{beamtime.beamline_lowercase}", beamtime.beamline.lower())
    )

    for k, v in additional_replacements.items():
        job_base_directory_str = job_base_directory_str.replace("{" + k + "}", v)

    if "{" in job_base_directory_str or "}" in job_base_directory_str:
        raise Exception(
            f"job base directory {job_base_directory_str} contains placeholder characters (so '{{' and '}}'), stopping",
        )

    return Path(job_base_directory_str)
