import re
from typing import NewType

from amarcord.db.constants import ATTRIBUTO_NOT_NAME_REGEX

AttributoId = NewType("AttributoId", str)


def sanitize_attributo_id(i: str) -> AttributoId:
    result = re.sub(ATTRIBUTO_NOT_NAME_REGEX, "_", i.lower())
    if not result:
        raise Exception(f"trying to sanitize {i} resulted in empty name")
    return AttributoId(result)
