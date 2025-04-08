from fastapi import APIRouter
from pint import UnitRegistry

from amarcord.web.json_models import JsonCheckStandardUnitInput
from amarcord.web.json_models import JsonCheckStandardUnitOutput

_UNIT_REGISTRY = UnitRegistry()
router = APIRouter()


@router.post("/api/unit", response_model_exclude_defaults=True)
async def check_standard_unit(
    input_: JsonCheckStandardUnitInput,
) -> JsonCheckStandardUnitOutput:
    unit_input = input_.input

    if unit_input == "":
        return JsonCheckStandardUnitOutput(
            input=unit_input,
            error="Unit empty",
            normalized=None,
        )

    try:
        return JsonCheckStandardUnitOutput(
            input=unit_input,
            error=None,
            normalized=f"{_UNIT_REGISTRY(unit_input):P}",
        )
    except:
        return JsonCheckStandardUnitOutput(
            input=unit_input,
            error="Invalid unit",
            normalized=None,
        )
