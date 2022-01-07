import datetime

from quart import request
from quart.json import JSONEncoder

from amarcord.json import JSONDict


async def quart_safe_json_dict() -> JSONDict:
    json_content = await request.get_json(force=True)
    assert isinstance(
        json_content, dict
    ), f"expected a dictionary for the request input, got {json_content}"
    return json_content


class CustomJSONEncoder(JSONEncoder):
    def default(self, object_):
        # The default ISO-format for JSON encoding isn't well-parseable, better to use good olde ISO!
        if isinstance(object_, datetime.datetime):
            return object_.isoformat(sep=" ", timespec="seconds")
        return JSONEncoder.default(self, object_)
