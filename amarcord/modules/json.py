from typing import Dict, List, Union

JSONArray = List["JSONValue"]
JSONValue = Union[int, str, float, bool, None, "JSONDict", "JSONArray"]
JSONDict = Dict[str, JSONValue]
