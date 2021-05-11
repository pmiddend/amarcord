from pathlib import Path

from pint import UnitRegistry

from amarcord.amici.p11.parser import parse_p11_info_file


def test_parse_info_file() -> None:
    ureg = UnitRegistry()
    info_file = parse_p11_info_file(Path(__file__).parent / "info.txt", ureg)

    assert info_file.aperture == 200 * ureg("micrometer")
    assert info_file.degrees_per_frame == 0.2 * ureg("degree")
    assert info_file.detector_distance == 155.5 * ureg("millimeter")
    assert info_file.energy == 12 * ureg("kiloelectron_volt")
    assert info_file.exposure_time == 0.03 * ureg("millisecond")
    assert info_file.filter_thickness == 0 * ureg("micrometer")
    assert info_file.filter_transmission_percent == 100.0
    assert info_file.focus == "Flat"
    assert info_file.frames == 1000
    assert info_file.resolution == 1.35 * ureg("angstrom")
    assert info_file.run_name == "bmx001_pos06_00"
    assert info_file.run_type == "regular"
    assert info_file.start_angle == 0.0 * ureg("degree")
    assert info_file.wavelength == 1.033 * ureg("angstrom")
    assert info_file.ring_current == 99.046 * ureg("milliampere")
    assert len(info_file.flux_rows) == 9
    assert info_file.flux_rows[0].focus == "Flat"
    assert info_file.flux_rows[0].beam_area == "200 x 200"
    assert info_file.flux_rows[0].flux == "2e12"
