import datetime
import hashlib
import os
import re
import zipfile
from bz2 import BZ2Compressor
from bz2 import BZ2Decompressor
from contextlib import contextmanager
from difflib import SequenceMatcher
from pathlib import Path
from statistics import variance
from typing import BinaryIO
from typing import Callable
from typing import Final
from typing import Generator
from typing import Iterable
from typing import Iterator
from typing import Sequence
from typing import TypeVar
from zoneinfo import ZoneInfo

import anyio

T = TypeVar("T")
U = TypeVar("U")

K = TypeVar("K")
V = TypeVar("V")


def str_to_int(s: str) -> int | None:
    try:
        return int(s)
    except:
        return None


def find_regex(s: str, regex: str, start: int) -> int:
    r = re.search(regex, s[start:])
    if r is None:
        return -1
    return r.start() + start


def rfind_regex(s: str, regex: str, start: int) -> int:
    r = find_regex(s[::-1], regex, len(s) - start - 1)
    if r < 0:
        return r
    return len(s) - r - 1


# See https://stackoverflow.com/a/17016257
def remove_duplicates_stable[T](seq: Iterable[T]) -> list[T]:
    return list(dict.fromkeys(seq))


def dict_union[K, V](a: Sequence[dict[K, V]]) -> dict[K, V]:
    if not a:
        return {}
    result = a[0].copy()
    for v in a[1:]:
        result.update(v)
    return result


def str_to_float(s: str) -> float | None:
    try:
        return float(s)
    except:
        return None


W = TypeVar("W")
X = TypeVar("X")


def retupled_keys[K, V, W, X](
    d: dict[K, dict[V, W]],
    f: Callable[[K, V], X],
) -> list[X]:
    return [
        f(table, attributo_id)
        for table, attributi in d.items()
        for attributo_id in attributi
    ]


def retuple_dict[K, V, W, X](
    d: dict[K, dict[V, W]], f: Callable[[K, V], X]
) -> dict[X, W]:
    return {
        f(table, attributo_id): values
        for table, attributi in d.items()
        for attributo_id, values in attributi.items()
    }


def create_intervals(xs: list[int]) -> Generator[tuple[int, int]]:
    if not xs:
        return
    sorted_xs = sorted(xs)
    interval_start = sorted_xs[0]
    last_element = interval_start
    for x in sorted_xs[1:]:
        if x != last_element + 1:
            yield interval_start, last_element
            interval_start = x
        last_element = x
    yield interval_start, sorted_xs[-1]


class UnexpectedEOFError(Exception):
    def __init__(self) -> None:
        super().__init__("Unexpected EOF")


def find_by[T](xs: list[T], by: Callable[[T], bool]) -> T | None:
    return next((x for x in xs if by(x)), None)


def contains[T](xs: list[T], by: Callable[[T], bool]) -> bool:
    return any(by(x) for x in xs)


def natural_key(string_: str) -> list[int | str]:
    """See https://blog.codinghorror.com/sorting-for-humans-natural-sort-order/"""
    return [int(s) if s.isdigit() else s for s in re.split(r"(\d+)", string_)]


def path_mtime(p: Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(
        p.stat().st_mtime,
        tz=datetime.UTC,
    )


def deglob_path(x: Path) -> Path:
    return Path(re.sub(r"\*.*$", "", str(x)))


class DontUpdate:
    pass


TriOptional = T | None | DontUpdate


def sha256_file(p: Path) -> str:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def sha256_file_bytes(p: Path) -> bytes:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).digest()


def sha256_combination(hashes: Iterable[bytes]) -> str:
    return hashlib.sha256(b"".join(hashes)).hexdigest()


def sha256_files(ps: Iterable[Path]) -> str:
    return sha256_combination(sha256_file_bytes(p) for p in ps)


def read_file_to_string(p: Path) -> str:
    with p.open("r") as f:
        return f.read()


def last_line_of_file(p: Path) -> str:
    with p.open("r") as f:
        # Be dumb for now, probably use this solution if the need arises:
        # https://stackoverflow.com/questions/3346430/what-is-the-most-efficient-way-to-get-first-and-last-line-of-a-text-file/3346788
        lines = f.readlines()
        if lines:
            return lines[-1]
        return ""


def safe_max[T, U](
    xs: Iterable[T],
    key: Callable[[T], U],  # pyright: ignore[reportInvalidTypeVarUse]
) -> T | None:
    try:
        # mypy wants Callable[[T], Union[SupportsDunderLT, SupportsDunderGT]] but that's internal
        return max(xs, key=key)  # type: ignore
    except ValueError:
        return None


def group_by[T, U](xs: Iterable[T], key: Callable[[T], U]) -> dict[U, list[T]]:
    result: dict[U, list[T]] = {}
    for x in xs:
        key_value = key(x)
        previous_values = result.get(key_value)
        if previous_values is None:
            result[key_value] = [x]
        else:
            previous_values.append(x)
    return result


def now_utc_unix_integer_millis() -> int:
    return int(
        datetime.datetime.now(datetime.UTC).replace(tzinfo=datetime.UTC).timestamp()
        * 1000,
    )


def last_existing_dir(p: Path) -> Path | None:
    if p.is_dir():
        return p
    next_ = p.parent
    if next_ == p:
        return None
    return last_existing_dir(p.parent)


def replace_illegal_path_characters(filename: str) -> str:
    # See https://stackoverflow.com/questions/1033424/how-to-remove-bad-path-characters-in-python
    return re.sub(r"[^\w\-_. ]", "_", filename)


def safe_variance(xs: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    return variance(xs)


def utc_datetime_to_local(value: datetime.datetime) -> datetime.datetime:
    current_tz = get_local_tz()
    return (
        value.replace(tzinfo=datetime.UTC).astimezone(current_tz).replace(tzinfo=None)
    )


def maybe_you_meant(s: str, strs: Iterable[str]) -> str:
    if not strs:
        return ""
    max_match, ratio = max(
        ((t, SequenceMatcher(None, s, t).ratio()) for t in strs),
        key=lambda x: x[1],
    )
    return f', maybe you meant "{max_match}"?' if ratio > 0.5 else ""


def first[T](xs: Iterable[T]) -> T | None:
    for x in xs:
        return x
    return None


def overwrite_interpreter(file_contents: str, interpreter: str) -> str:
    lines = file_contents.split("\n")
    lines[0] = interpreter
    return "\n".join(lines)


def check_consecutive(xs: Iterable[int]) -> tuple[int, int] | None:
    prev = None
    for x in xs:
        if prev is not None and prev != x - 1:
            return (prev, x)
        prev = x
    return None


def get_local_tz() -> ZoneInfo:
    return ZoneInfo(os.environ.get("AMARCORD_TZ", "Europe/Berlin"))


async def bz2_compress_async(
    original_path: anyio.Path, bz2_file_path: anyio.Path
) -> None:
    copy_bufsize: Final = 64 * 1024

    async with (
        await original_path.open("rb") as original_obj,
        await bz2_file_path.open("wb") as bz2_file_obj,
    ):
        compressor = BZ2Compressor()
        # This is almost a carbon-copy of "shutil.copyfileobj", but
        # with more async sprinkled in
        while buf := await original_obj.read(copy_bufsize):
            await bz2_file_obj.write(compressor.compress(buf))
        await bz2_file_obj.write(compressor.flush())


async def bz2_decompress_async(
    bz2_file_path: anyio.Path, target_path: anyio.Path
) -> None:
    copy_bufsize: Final = 64 * 1024

    async with (
        await target_path.open("wb") as target_obj,
        await bz2_file_path.open("rb") as bz2_file_obj,
    ):
        decompressor = BZ2Decompressor()
        # This is almost a carbon-copy of "shutil.copyfileobj", but
        # with more async sprinkled in
        while buf := await bz2_file_obj.read(copy_bufsize):
            await target_obj.write(decompressor.decompress(buf))


async def zip_compress_async(
    zip_filename: anyio.Path,
    base_dir_relative: str,
    root_dir: anyio.Path,
) -> anyio.Path:
    archive_dir = zip_filename.parent

    await archive_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_filename, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        arcname = base_dir_relative
        base_dir_absolute = root_dir / base_dir_relative
        if arcname != Path.cwd():
            zf.write(base_dir_absolute, base_dir_relative)
        async for dirpath, dirnames, filenames in base_dir_absolute.walk():
            arcdirpath = dirpath
            arcdirpath = arcdirpath.relative_to(root_dir)
            for name in sorted(dirnames):
                path = dirpath / name
                arcname = arcdirpath / name
                zf.write(path, arcname)
            for name in filenames:
                path = dirpath / name
                if await path.is_file():
                    arcname = arcdirpath / name
                    zf.write(path, arcname)

    return await zip_filename.absolute()


async def zip_decompress_async(file_obj: BinaryIO, extract_dir: anyio.Path) -> None:
    copy_bufsize: Final = 64 * 1024

    with zipfile.ZipFile(file_obj) as zip_obj:
        for info in zip_obj.infolist():
            name = info.filename

            # don't extract absolute paths or ones with .. in them
            if name.startswith("/") or ".." in name:
                continue

            targetpath = extract_dir / anyio.Path(name)

            await targetpath.parent.mkdir(parents=True, exist_ok=True)

            if not name.endswith("/"):
                # file
                with zip_obj.open(name, "r") as source_sync_obj:
                    async with await targetpath.open("wb") as target_obj:
                        while buf := source_sync_obj.read(copy_bufsize):
                            await target_obj.write(buf)


@contextmanager
def temporary_env(name: str, new_value: str) -> Iterator[None]:
    old_value = os.environ.get(name)

    os.environ[name] = new_value
    try:
        yield
    finally:
        if old_value is not None:
            os.environ[name] = old_value
        else:
            os.environ.pop(name, None)


async def rmdir_async(p: anyio.Path) -> None:
    async for root, dirs, files in p.walk(top_down=False):
        for file in files:
            await (root / file).unlink()
        for directory in dirs:
            await (root / directory).rmdir()
    await p.rmdir()
