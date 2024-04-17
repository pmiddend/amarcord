from typing import Final

import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from amarcord.db import orm
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.orm_utils import create_new_user_configuration
from amarcord.db.orm_utils import retrieve_latest_config
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonUserConfig
from amarcord.web.json_models import JsonUserConfigurationSingleOutput

router = APIRouter()
logger = structlog.stdlib.get_logger(__name__)
USER_CONFIGURATION_AUTO_PILOT: Final = "auto-pilot"
USER_CONFIGURATION_ONLINE_CRYSTFEL: Final = "online-crystfel"
USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID: Final = "current-experiment-type-id"
KNOWN_USER_CONFIGURATION_VALUES: Final = [
    USER_CONFIGURATION_AUTO_PILOT,
    USER_CONFIGURATION_ONLINE_CRYSTFEL,
    USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID,
]


def encode_user_configuration(c: orm.UserConfiguration) -> JsonUserConfig:
    return JsonUserConfig(
        online_crystfel=c.use_online_crystfel,
        auto_pilot=c.auto_pilot,
        current_experiment_type_id=c.current_experiment_type_id,
    )


@router.get(
    "/api/user-config/{beamtimeId}/{key}",
    tags=["config"],
    response_model_exclude_defaults=True,
)
async def read_user_configuration_single(
    beamtimeId: BeamtimeId, key: str, session: AsyncSession = Depends(get_orm_db)
) -> JsonUserConfigurationSingleOutput:
    user_configuration = await retrieve_latest_config(session, beamtimeId)
    if key == USER_CONFIGURATION_AUTO_PILOT:
        return JsonUserConfigurationSingleOutput(
            value_bool=user_configuration.auto_pilot, value_int=None
        )
    if key == USER_CONFIGURATION_ONLINE_CRYSTFEL:
        return JsonUserConfigurationSingleOutput(
            value_bool=user_configuration.use_online_crystfel, value_int=None
        )
    if key == USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID:
        return JsonUserConfigurationSingleOutput(
            value_int=user_configuration.current_experiment_type_id, value_bool=None
        )
    raise Exception(
        f"Couldn't find config key {key}, only know "
        + ", ".join(f'"{x}"' for x in KNOWN_USER_CONFIGURATION_VALUES)
    )


@router.patch(
    "/api/user-config/{beamtimeId}/{key}/{value}",
    tags=["config"],
    response_model_exclude_defaults=True,
)
async def update_user_configuration_single(
    beamtimeId: BeamtimeId,
    key: str,
    value: str,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonUserConfigurationSingleOutput:
    async with session.begin():
        user_configuration = await retrieve_latest_config(session, beamtimeId)
        new_config = create_new_user_configuration(user_configuration)
        session.add(new_config)
        if key == USER_CONFIGURATION_AUTO_PILOT:
            new_value = value == "True"
            new_config.auto_pilot = new_value
            await session.commit()
            return JsonUserConfigurationSingleOutput(
                value_bool=new_value, value_int=None
            )
        if key == USER_CONFIGURATION_ONLINE_CRYSTFEL:
            new_value = value == "True"
            new_config.use_online_crystfel = new_value
            await session.commit()

            return JsonUserConfigurationSingleOutput(
                value_bool=new_value, value_int=None
            )
        if key == USER_CONFIGURATION_CURRENT_EXPERIMENT_TYPE_ID:
            logger.info(f"setting current experiment type to {value}")
            new_config.current_experiment_type_id = int(value)
            await session.commit()
            return JsonUserConfigurationSingleOutput(
                value_int=int(value), value_bool=None
            )
        raise Exception(
            f"Couldn't find config key {key}, only know "
            + ", ".join(f'"{x}"' for x in KNOWN_USER_CONFIGURATION_VALUES)
        )
