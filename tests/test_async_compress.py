import bz2
import filecmp
import shutil
from filecmp import dircmp
from pathlib import Path

import anyio

from amarcord.util import bz2_compress_async
from amarcord.util import bz2_decompress_async
from amarcord.util import zip_compress_async
from amarcord.util import zip_decompress_async


def _bz2_compress_sync(original_path: Path, bz2_file_path: Path) -> None:
    with (
        bz2.open(bz2_file_path, "wb") as bz2_file_obj,
        original_path.open("rb") as source_file_obj,
    ):
        shutil.copyfileobj(source_file_obj, bz2_file_obj)


def _bz2_decompress_sync(bz2_file_path: Path, target_path: Path) -> None:
    with (
        bz2.open(bz2_file_path, "rb") as bz2_file_obj,
        target_path.open("wb") as target_file_obj,
    ):
        shutil.copyfileobj(bz2_file_obj, target_file_obj)


async def test_bz2_async_compress(tmp_path: Path) -> None:
    input_file = tmp_path / "test.txt"
    with input_file.open("w") as f:
        f.write("line 123")

    sync_compress_file = tmp_path / "test_sync.bz2"
    _bz2_compress_sync(input_file, sync_compress_file)
    async_compress_file = tmp_path / "test_async.bz2"
    await bz2_compress_async(anyio.Path(input_file), anyio.Path(async_compress_file))
    assert filecmp.cmp(sync_compress_file, async_compress_file)

    sync_decompress_file = tmp_path / "sync_test.txt"
    _bz2_decompress_sync(sync_compress_file, sync_decompress_file)

    async_decompress_file = tmp_path / "async_test.txt"
    await bz2_decompress_async(
        anyio.Path(async_compress_file), anyio.Path(async_decompress_file)
    )
    assert filecmp.cmp(sync_decompress_file, async_decompress_file)


async def test_zip_async_compress(tmp_path: Path) -> None:
    root_dir = tmp_path / "root"
    base_dir = root_dir / "base"
    sub_dir = base_dir / "sub"
    sub_dir.mkdir(parents=True)

    with (base_dir / "f1").open("w") as f:
        f.write("line 123")

    with (sub_dir / "f2").open("w") as f:
        f.write("line 1234")

    output_file_sync = tmp_path / "output_sync.zip"
    shutil.make_archive(
        str(output_file_sync.with_suffix("")), "zip", root_dir=root_dir, base_dir="base"
    )

    output_file_async = tmp_path / "output_async.zip"
    await zip_compress_async(
        anyio.Path(output_file_async),
        base_dir_relative="base",
        root_dir=anyio.Path(root_dir),
    )
    assert filecmp.cmp(output_file_sync, output_file_async)

    with output_file_async.open("rb") as f:
        await zip_decompress_async(f, anyio.Path(tmp_path / "extracted-async"))
    shutil.unpack_archive(output_file_sync, tmp_path / "extracted-sync")

    directory_comparison = dircmp(
        tmp_path / "extracted-sync", tmp_path / "extracted-async"
    )
    assert not directory_comparison.left_only
    assert not directory_comparison.right_only
    assert not directory_comparison.diff_files
