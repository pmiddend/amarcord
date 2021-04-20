from pathlib import Path

from amarcord.amici.cheetah import cheetah_read_crawler_config_file
from amarcord.amici.cheetah import cheetah_read_crawler_runs_table
from amarcord.amici.cheetah import cheetah_read_recipe


def _read_test_file(p: Path) -> Path:
    return Path(__file__).parent / p


def test_cheetah_read_config_file() -> None:
    config_result = cheetah_read_crawler_config_file(
        _read_test_file(Path("gui") / "crawler.config")
    )
    assert config_result.hdf5dir == Path("../hdf5/")


def test_cheetah_read_recipe() -> None:
    recipe_result = cheetah_read_recipe(
        _read_test_file(Path("hdf5") / "r0213-cry41" / "agipd-snr8pix1.ini")
    )

    assert recipe_result.adc == 200
    assert recipe_result.algorithm == 8
    assert recipe_result.local_bg_radius is None


def test_cheetah_read_table() -> None:
    table_result = cheetah_read_crawler_runs_table(
        _read_test_file(Path("gui") / "crawler.txt")
    )

    # Insanely stupid way of testing, simply by picking some random file and asserting the parser
    # doesn't do anything wrong. But there you go. :)
    assert len(table_result) == 121
    assert table_result[0].hdf5_directory is None
    assert table_result[9].hdf5_directory is None
