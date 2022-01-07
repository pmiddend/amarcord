import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import yaml

from amarcord.amici.xfel.karabo_attributi import KaraboAttributi
from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_expected_attributi import KaraboExpectedAttributi
from amarcord.amici.xfel.karabo_processor import KaraboProcessor
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KaraboConfiguration:
    attributi: KaraboAttributi
    expected_attributi: KaraboExpectedAttributi
    ignore_entry: Dict[KaraboSource, List[str]]


def _validate_type(identifier: str, type_: str) -> None:
    list_re = re.fullmatch(
        r"image|int|decimal|str|datetime|list\[(int|decimal|str)]", type_
    )
    if not list_re:
        raise ValueError(
            f'attributo "{identifier}": type/karabo_type must be str, int, decimal, datetime or a list of int or '
            f"decimal, is {type} "
        )


def _parse_attributo(
    identifier: str,
    global_source: Optional[str],
    global_action: Optional[str],
    ai_content: Dict[str, Any],
) -> KaraboAttributo:
    type_ = ai_content.get("type", None)

    if type_ is None:
        raise ValueError(f'Attributo "{identifier}": has no type')

    _validate_type(identifier, type_)

    karabo_type = ai_content.get("karabo_type", None)
    if karabo_type is not None:
        _validate_type(identifier, karabo_type)

    processor = ai_content.get("processor", None)
    if processor is not None and processor not in [x.value for x in KaraboProcessor]:
        processor_values = "' or '".join(s.value for s in KaraboProcessor)
        raise ValueError(
            f"Attributo \"{identifier}\": processor must be either '{processor_values}', is {processor}"
        )
    if processor is not None:
        processor = KaraboProcessor(processor)

    action = ai_content.get("action", global_action)
    if action is not None and action not in [x.value for x in KaraboAttributoAction]:
        action_values = "' or '".join(s.value for s in KaraboAttributoAction)
        raise ValueError(
            f"Attributo \"{identifier}\": action must be either '{action_values}', is {action}"
        )
    if action is not None:
        action = KaraboAttributoAction(action)
    else:
        action = KaraboAttributoAction.STORE_LAST

    source = ai_content.get("source", global_source)

    if source is None:
        raise ValueError(
            f'Attributo "{identifier}": has no source and no global source given'
        )

    role = ai_content.get("role", None)
    if role is not None and role not in [x.value for x in KaraboSpecialRole]:
        role_values = "' or '".join(s.value for s in KaraboSpecialRole)
        raise ValueError(
            f"Attributo {identifier}: Role must be either '{role_values}', is {role}"
        )
    if role is not None:
        role = KaraboSpecialRole(role)

    return KaraboAttributo(
        identifier=identifier,
        source=source,
        key=ai_content["key"],
        description=ai_content.get("description", None),
        type_=type_,
        karabo_type=karabo_type,
        store=ai_content.get("store", True),
        processor=processor,
        action=action,
        unit=ai_content.get("unit", None),
        filling_value=ai_content.get("filling_value", None),
        value=None,
        role=role,
    )


def parse_karabo_configuration_file(fn: Path) -> KaraboConfiguration:
    with fn.open("r") as f:
        return parse_karabo_configuration(yaml.load(f, Loader=yaml.SafeLoader))


def parse_karabo_configuration(configuration: Dict[str, Any]) -> KaraboConfiguration:
    _validate_version(configuration)

    attributi: KaraboAttributi = {}
    karabo_expected_entry: KaraboExpectedAttributi = {}

    for (
        gi,
        gi_content,
    ) in configuration["attributi_definition"].items():
        source, action = None, None

        for (
            ai,
            ai_content,
        ) in gi_content.items():

            # source and action can be set globally, for the entire group
            if ai == "source":
                source = ai_content
            elif ai == "action":
                action = ai_content

            if isinstance(ai_content, dict):
                if gi not in attributi:
                    attributi[gi] = {}

                if ai in attributi[gi]:
                    logger.warning(
                        "duplicate attributo for source %s and key %s", gi, ai
                    )
                    continue
                attributi[gi][ai] = _parse_attributo(ai, source, action, ai_content)

    # build the corresponding Karabo bridge expected entry
    for group_name, group in attributi.items():
        for attributo in group.values():
            if attributo.source not in karabo_expected_entry:
                karabo_expected_entry[attributo.source] = {
                    attributo.key: {"attributo": attributo, "group": group_name}
                }
            else:
                karabo_expected_entry[attributo.source].update(
                    {attributo.key: {"attributo": attributo, "group": group_name}}
                )

    ignore_entries = configuration.get("ignore_entry", {})
    for ignore_source, ignore_entry in ignore_entries.items():
        if not isinstance(ignore_entry, list):
            raise Exception(
                f'invalid config file, expected a list of entries in "ignore_entry" for '
                f'source "{ignore_source}", got {type(ignore_entry)}'
            )

    return KaraboConfiguration(
        attributi=attributi,
        expected_attributi=karabo_expected_entry,
        ignore_entry=ignore_entries,
    )


def _validate_version(configuration: Dict[str, Any]) -> None:
    config_version = configuration.get("config_version", None)
    if config_version is None:
        raise Exception(
            'invalid config format, needs "config_version" of at least 2, didn\'t find any'
        )
    if not isinstance(config_version, int):
        raise Exception(
            f'invalid config format, needs "config_version" of at least 2, version wasn\'t a "'
            f"string but {type(config_version)}"
        )
    if config_version < 2:
        raise Exception(
            f'invalid config format, needs "config_version" of at least 2, version was {config_version}'
        )
