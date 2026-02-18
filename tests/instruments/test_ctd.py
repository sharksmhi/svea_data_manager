from datetime import date
from pathlib import Path

import pytest

from svea_data_manager.instruments.ctd import CtdResource


@pytest.mark.parametrize(
    "given_suffix, expected_match",
    (
        ("", False),
        (".csv", False),
        (".jpg", False),
        (".cnv", True),
        (".bl", True),
        (".btl", True),
        (".hdr", True),
        (".hex", True),
        (".ros", True),
        (".xmlcon", True),
        (".xml", True),
        (".zip", True),
        (".cnv", True),
        (".txt", True),
    ),
)
def test_ctdresource_accepted_suffixes(tmp_path, given_suffix: str, expected_match: bool):
    # Given an allowed file stem and a suffix
    given_filename = Path("SBE09_1234_20250714_1853_77SE_17_0570").with_suffix(
        given_suffix
    )

    # When giving it to CTDResource
    resource = CtdResource.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("SBE12_1234_20260101_1234_12_SE_1234", True),
        ("uSBE12_1234_20260101_1234_12_SE_1234", True),
        ("dSBE12_1234_20260101_1853_12_SE_1234", True),
        ("SBE12_1234_20260101_1853_12SE_12_1234", True),
        ("uSBE12_1234_20260101_1853_12SE_12_1234", True),
        ("SBE12_1234_20260101_1853_12SE_12_1234_psa_config", True),
        ("uSBE12_1234_20260101_1853_12SE_12_1234_psa_config", True),
    ),
)
def test_ctdresource_accepted_filenames(tmp_path, given_file_stem, expected_match: bool):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to CTDResource
    resource = CtdResource.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_ctdresource_extracts_attributes_from_filename(tmp_path):
    # Given a filename made up of specific components
    given_prefix = "u"
    given_instrument = "SBE12"
    given_instrument_number = "1234"
    given_year = "2026"
    given_month = "01"
    given_day = "02"
    given_hour = "20"
    given_minute = "30"
    given_ship = "12AB"
    given_cruise = "23"
    given_serial_number = "3456"

    given_filename = Path(
        f"{given_prefix}{given_instrument}_{given_instrument_number}_"
        f"{given_year}{given_month}{given_day}_{given_hour}{given_minute}_"
        f"{given_ship}_{given_cruise}_{given_serial_number}.txt"
    )

    # When giving it to CTDResource
    resource = CtdResource.from_source_file(tmp_path, given_filename)

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "prefix",
        "instrument",
        "instrument_number",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "ship",
        "cruise",
        "serno",
    }

    # And the values are intact
    assert resource.attributes["prefix"] == given_prefix
    assert resource.attributes["instrument"] == given_instrument
    assert resource.attributes["instrument_number"] == given_instrument_number
    assert resource.attributes["year"] == given_year
    assert resource.attributes["month"] == given_month
    assert resource.attributes["day"] == given_day
    assert resource.attributes["hour"] == given_hour
    assert resource.attributes["minute"] == given_minute
    assert resource.attributes["ship"] == given_ship
    assert resource.attributes["cruise"] == given_cruise
    assert resource.attributes["serno"] == given_serial_number


@pytest.mark.parametrize(
    "given_filename, expected_date",
    (
        ("SBE12_1234_20240101_1234_12_SE_1234.txt", date(2024, 1, 1)),
        ("SBE12_1234_20250215_1234_12_SE_1234.txt", date(2025, 2, 15)),
        ("SBE12_1234_20260331_1234_12_SE_1234.txt", date(2026, 3, 31)),
    ),
)
def test_ctdresource_can_identify_date(tmp_path, given_filename: str, expected_date):
    # Given a filename
    # When giving it to CTDResource
    resource = CtdResource.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_date
