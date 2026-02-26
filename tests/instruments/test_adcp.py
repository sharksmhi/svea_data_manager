from datetime import date
from pathlib import Path

import pytest

from svea_data_manager.frameworks.exceptions import ConfigurationError
from svea_data_manager.instruments.adcp import (
    Adcp,
    AdcpResourceProcessed,
    AdcpResourceRaw,
)


def test_adcp_raises_without_target_directory():
    # Given a configuration without target_directory.
    given_config = {"source_directory": "/any/path/"}
    assert "target_directory" not in given_config

    # When creating Adcp
    # Then it raises an exception
    with pytest.raises(ConfigurationError):
        Adcp(given_config)


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("ADCP_12AB_2026_01_a_b", False),
        ("ADCPa_12AB_2026_01_a_b", True),
        ("ADCPb_12AB_2026_01_b", True),
        ("ADCPc_SMHI_c_2026_b", True),
        ("ADCPd_SMHI_c2026_b", True),
    ),
)
def test_adcpresourceraw_accepted_filenames(tmp_path, given_file_stem, expected_match):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to ADCPResourceRaw
    resource = AdcpResourceRaw.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_adcpresourceraw_extracts_attributes_from_filename(tmp_path):
    # Given a filename made up of specific components
    given_instrument = "ADCPa"
    given_ship = "12AB"
    given_year = "2026"
    given_cruise = "23"
    given_counter = "01"
    given_nr = "02"
    given_suffix = ".txt"

    given_filename = Path(
        f"{given_instrument}_{given_ship}_"
        f"{given_year}_{given_cruise}_{given_counter}_{given_nr}{given_suffix}"
    )

    # When giving it to ADCPResourceRaw
    resource = AdcpResourceRaw.from_source_file(tmp_path, given_filename)

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "instrument",
        "ship",
        "year",
        "cruise",
        "counter",
        "nr",
        "suffix",
    }

    # And the values are intact
    assert resource.attributes["instrument"] == given_instrument
    assert resource.attributes["ship"] == given_ship
    assert resource.attributes["year"] == given_year
    assert resource.attributes["cruise"] == given_cruise
    assert resource.attributes["counter"] == given_counter
    assert resource.attributes["nr"] == given_nr


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("ADCP_12AB_2026_01_processed", False),
        ("ADCPa_12AB_2026_01_processed", True),
        ("ADCP_12AB_2026_01_utdata", False),
        ("ADCPb_12AB_2026_01_utdata", True),
    ),
)
def test_adcpresourceprocessed_accepted_filenames(
    tmp_path, given_file_stem, expected_match
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to ADCPResourceProcessed
    resource = AdcpResourceProcessed.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_adcpresourceprocessed_extracts_attributes_from_filename(tmp_path):
    # Given a filename made up of specific components
    given_instrument = "ADCPa"
    given_ship = "12AB"
    given_year = "2026"
    given_cruise = "23"
    given_suffix = ".anysuffix"

    given_filename = Path(
        f"{given_instrument}_{given_ship}_"
        f"{given_year}_{given_cruise}_processed{given_suffix}"
    )

    # When giving it to ADCPResourceProcessed
    resource = AdcpResourceProcessed.from_source_file(tmp_path, given_filename)

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "instrument",
        "ship",
        "year",
        "cruise",
        "suffix",
    }

    # And the values are intact
    assert resource.attributes["instrument"] == given_instrument
    assert resource.attributes["ship"] == given_ship
    assert resource.attributes["year"] == given_year
    assert resource.attributes["cruise"] == given_cruise
    assert resource.attributes["suffix"] == given_suffix


# The mapping code is never used due to ambiguous logic
@pytest.mark.parametrize(
    "given_date, expected_cruise",
    (
        pytest.param(date(2022, 1, 9), "?", marks=pytest.mark.xfail),
        pytest.param(date(2022, 1, 10), "1", marks=pytest.mark.xfail),
        pytest.param(date(2022, 8, 20), "?", marks=pytest.mark.xfail),
    ),
)
def test_cruise_mapping_from_log(dir_factory, given_date, expected_cruise):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given there is a .hdr file
    given_file = given_read_dir / "ADCPb_SMHI_jan_2026_1.hdr"
    given_file.write_text("Content")

    # Given there is a .log file with a given date inside
    given_log_file = given_read_dir / "ADCPb_SMHI_jan_2026_1.log"
    given_log_file.write_text(f"{given_date.strftime('%Y/%m/%d: Something happened')}")

    # Given ADCP
    given_config = {
        "source_directory": str(given_read_dir),
        "target_directory": str(given_write_dir),
    }

    given_adcp = Adcp(given_config)
    given_adcp.read_packages()

    # When transforming the packag
    given_adcp.transform_packages()

    # Then cruise has expected value somewhere
    assert False
