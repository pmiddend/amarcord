from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import TypeVar
from typing import Union

import yaml

T = TypeVar("T")


@dataclass(frozen=True)
class ConfigDict:
    value: Dict[str, "ConfigAST"]


@dataclass(frozen=True)
class ConfigList:
    value: List["ConfigAST"]

    def __repr__(self) -> str:
        return (
            "list of " + (" or ".join(str(s) for s in self.value))
            if self.value
            else "empty list"
        )


@dataclass(frozen=True)
class ConfigInt:
    def __repr__(self) -> str:
        return "integer"


@dataclass(frozen=True)
class ConfigStr:
    def __repr__(self) -> str:
        return "string"


@dataclass(frozen=True)
class ConfigBool:
    def __repr__(self) -> str:
        return "boolean"


@dataclass(frozen=True)
class ConfigNull:
    def __repr__(self) -> str:
        return "null"


@dataclass(frozen=True)
class ConfigFloat:
    def __repr__(self) -> str:
        return "float"


@dataclass(frozen=True)
class Path:
    value: List[str]

    def __repr__(self) -> str:
        return "/".join(self.value)

    def __truediv__(self, other: str) -> "Path":
        return Path(self.value + [other])


ConfigAST = Union[
    ConfigDict, ConfigStr, ConfigInt, ConfigBool, ConfigFloat, ConfigList, ConfigNull
]


@dataclass(frozen=True)
class SchemaDict:
    value: Dict[str, "SchemaAST"]

    def __repr__(self) -> str:
        return "dictionary"


@dataclass(frozen=True)
class SchemaList:
    value: "SchemaAST"

    def __repr__(self) -> str:
        return f"list of {self.value}"


@dataclass(frozen=True)
class SchemaInt:
    def __repr__(self) -> str:
        return "integer"


@dataclass(frozen=True)
class SchemaNone:
    def __repr__(self) -> str:
        return "none"


@dataclass(frozen=True)
class SchemaBool:
    def __repr__(self) -> str:
        return "boolean"


@dataclass(frozen=True)
class SchemaStr:
    def __repr__(self) -> str:
        return "string"


@dataclass(frozen=True)
class SchemaFloat:
    def __repr__(self) -> str:
        return "float"


@dataclass(frozen=True)
class SchemaUnion:
    sub_types: List["SchemaAST"]


SchemaAST = Union[
    SchemaDict,
    SchemaInt,
    SchemaStr,
    SchemaFloat,
    SchemaBool,
    SchemaList,
    SchemaUnion,
    SchemaNone,
]


def typed_dict_to_schema(t: Any) -> SchemaAST:
    if t == type(None):
        return SchemaNone()
    if t == int:
        return SchemaInt()
    if t == str:
        return SchemaStr()
    if t == bool:
        return SchemaBool()
    if t == float:
        return SchemaFloat()
    if "__origin__" in t.__dict__:
        if t.__origin__ == list:
            return SchemaList(typed_dict_to_schema(t.__args__[0]))
        if t.__origin__ == Union:
            return SchemaUnion(
                [typed_dict_to_schema(subtype) for subtype in t.__args__]
            )
    if "__annotations__" not in t.__dict__:
        raise Exception(f"invalid type {type(t)} encountered")
    # now assume we have a dict (TypedDict, that is)
    result: Dict[str, SchemaAST] = {}
    for k, v in t.__annotations__.items():
        result[k] = typed_dict_to_schema(v)
    return SchemaDict(result)


InferenceResult = Union[ConfigAST, List[str]]


def _infer_type_dict(path: Path, d: Dict[Any, Any]) -> InferenceResult:
    errors: List[str] = []
    result: Dict[str, ConfigAST] = {}
    for k, v in d.items():
        if not isinstance(k, str):
            errors.append(
                f"{path}: found dictionary, but key isn't a string, it's {type(k)}"
            )
            continue
        subresult = infer_type(path / k, v)
        if isinstance(subresult, list):
            errors.extend(subresult)
            continue
        result[k] = subresult
    if errors:
        return errors
    return ConfigDict(result)


def _infer_type_list(path: Path, d: List[Any]) -> InferenceResult:
    errors: List[str] = []
    result: List[ConfigAST] = []
    for idx, k in enumerate(d):
        subpath = path / f"index {idx}"
        subresult = infer_type(subpath, k)
        if isinstance(subresult, list):
            errors.extend(subresult)
            continue
        result.append(subresult)
    if errors:
        return errors
    return ConfigList(result)


def infer_type(path: Path, d: Any) -> InferenceResult:
    if isinstance(d, dict):
        return _infer_type_dict(path, d)
    if isinstance(d, list):
        return _infer_type_list(path, d)
    if isinstance(d, bool):
        return ConfigBool()
    if isinstance(d, int):
        return ConfigInt()
    if isinstance(d, float):
        return ConfigFloat()
    if isinstance(d, str):
        return ConfigStr()
    if d is None:
        return ConfigNull()
    return [f"{path}: invalid type {type(d)} found"]


def is_optional(s: SchemaAST) -> bool:
    return isinstance(s, SchemaUnion) and SchemaNone() in s.sub_types


TypeErrors = List[Tuple[Path, str]]


def typecheck_dict(path: Path, instance: ConfigAST, schema: SchemaDict) -> TypeErrors:
    if not isinstance(instance, ConfigDict):
        return [(path, f"expected dictionary, got {instance}")]
    errors: List[Tuple[Path, str]] = []
    for schema_key, schema_value in schema.value.items():
        if is_optional(schema_value) and schema_key not in instance.value:
            continue
        if not is_optional(schema_value) and schema_key not in instance.value:
            errors.append((path, f'key "{schema_key} is mandatory but is not present"'))
            continue
        instance_value = instance.value[schema_key]
        errors.extend(typecheck(path / schema_key, instance_value, schema_value))
    return errors


def typecheck_list(path: Path, instance: ConfigAST, schema: SchemaList) -> TypeErrors:
    if not isinstance(instance, ConfigList):
        return [(path, f"expected list, got {instance}")]
    # empty list encountered - type checks with everything
    if not instance.value:
        return []
    # Here we have to ensure that the schema has "more types" than the instance.
    # In other words, the schema must be a superset of the instance.
    # In other words, for every "Config" type we need a typechecking "Schema" type.
    errors: TypeErrors = []
    for instance_type in instance.value:
        if typecheck(path, instance_type, schema.value) != []:
            errors.append(
                (path, f"expected {schema}, cannot unify with list of {instance_type}")
            )
    if errors:
        return errors
    return []


def typecheck(path: Path, instance: ConfigAST, schema: SchemaAST) -> TypeErrors:
    if isinstance(schema, SchemaDict):
        return typecheck_dict(path, instance, schema)
    if isinstance(schema, SchemaInt):
        if isinstance(instance, ConfigInt):
            return []
        return [(path, f"expected {ConfigInt()}, got {instance}")]
    if isinstance(schema, SchemaStr):
        if isinstance(instance, ConfigStr):
            return []
        return [(path, f"expected {ConfigStr()}, got {instance}")]
    if isinstance(schema, SchemaFloat):
        if isinstance(instance, ConfigFloat):
            return []
        return [(path, f"expected {ConfigFloat()}, got {instance}")]
    if isinstance(schema, SchemaBool):
        if isinstance(instance, ConfigBool):
            return []
        return [(path, f"expected {ConfigBool()}, got {instance}")]
    if isinstance(schema, SchemaList):
        return typecheck_list(path, instance, schema)
    if isinstance(schema, SchemaUnion):
        for subtype in schema.sub_types:
            sub_errors = typecheck(path, instance, subtype)
            if not sub_errors:
                return []
        sub_type_str = ", ".join(str(s) for s in schema.sub_types)
        return [(path, f"expected one of {sub_type_str}; got {instance}")]
    # Remaining type: None; every type positively checks for the None type
    return []


def validate_dict(input_yaml: Any, t: Any) -> Union[List[str], T]:
    inferred_type = infer_type(Path([]), input_yaml)
    if isinstance(inferred_type, list):
        return inferred_type
    errors = typecheck(Path([]), inferred_type, typed_dict_to_schema(t))
    if errors:
        return [f"{p}: {e}" for p, e in errors]
    return input_yaml  # type: ignore


def load_and_validate(s: str, t: Any) -> Union[List[str], T]:
    yaml_dict = yaml.load(s, Loader=yaml.SafeLoader)
    inferred_type = infer_type(Path([]), yaml_dict)
    if isinstance(inferred_type, list):
        return inferred_type
    errors = typecheck(Path([]), inferred_type, typed_dict_to_schema(t))
    if errors:
        return [f"{p}: {e}" for p, e in errors]
    return yaml_dict  # type: ignore
