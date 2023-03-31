from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, eq=True)
class SimpleURI:
    scheme: str
    parameters: dict[str, str]

    def string_parameter(self, key: str) -> None | str:
        return self.parameters.get(key)

    def path_parameter(self, key: str) -> None | Path:
        result = self.parameters.get(key)
        if result is not None:
            return Path(result)
        return None

    def bool_parameter(self, key: str) -> None | bool:
        result = self.parameters.get(key)
        if result is None:
            return None
        return result.lower() == "true"


def parse_simple_uri(s: str) -> str | SimpleURI:
    match s.split(":", maxsplit=1):
        case [_]:
            return "':' character not found in simple URI; expecting something like \"scheme:name=value|name2=value2\""
        case [scheme, rest]:
            if not scheme.strip():
                return "scheme (what comes before ':') is empty"
            if not rest.strip():
                return SimpleURI(scheme.strip(), {})
            rest_parts: dict[str, str] = {}
            for rest_part_idx, rest_part in enumerate(rest.split("|")):
                # pyright says list[str] wasn't handled, but maxsplit prevents this case from happening
                match rest_part.split("=", maxsplit=1):  #  pyright: ignore
                    case [_]:
                        return f'argument {rest_part_idx}: cannot find \'=\' character, expected something like "key=value", but got "{rest_part}"'
                    case [key, value]:
                        if not key.strip():
                            return f'argument {rest_part_idx}: key seems to be empty, expected something like "key=value", but got "{rest_part}"'
                        if key.strip() in rest_parts:
                            return f'argument {rest_part_idx}: key "{key}" is already occurred in the simple URI'
                        rest_parts[key.strip()] = value
            return SimpleURI(scheme.strip(), rest_parts)
        case _:
            return f'got a weird string here, cannot parse "{s}"'
