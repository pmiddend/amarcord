from pathlib import Path

from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.web.json_models import JsonBeamtime


def determine_output_directory(
    beamtime: JsonBeamtime,
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
            str(datetime_from_attributo_int(beamtime.start).year),
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
