from datetime import date
from pathlib import Path

import pytest

from svea_data_manager.frameworks.exceptions import ConfigurationError
from svea_data_manager.instruments.ferrybox import (
    Ferrybox,
    FerryboxResourceCO2,
    FerryboxResourceRaw,
    FerryboxResourceWiski,
)


def test_ferrybox_raises_without_target_directory():
    # Given a configuration without target_directory.
    given_config = {"source_directory": "/any/path/"}
    assert "target_directory" not in given_config

    # When creating Ferrybox
    # Then it raises an exception
    with pytest.raises(ConfigurationError):
        Ferrybox(given_config)


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("a_2026-02-12", True),
        ("b_2026-02-12a", True),
        ("c_2026-02-12_13-45", True),
        ("d 20260212 124530", True),
        ("e_20260212", True),
    ),
)
def test_ferryboxresourceraw_accepted_filenames(
    wordless_tmp_path, given_file_stem, expected_match: bool
):
    # Given an allowed directory path
    given_directory_path = wordless_tmp_path / "ferrybox"

    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to FerryboxResourceRaw
    resource = FerryboxResourceRaw.from_source_file(given_directory_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_directory_path, expected_match",
    (
        ("", False),
        ("ferrybox", True),
        ("expedition/FeRrYbOx/data", True),
        ("ferrybox/toFTP", False),
        ("ferrybox/FTP_temp", False),
        # Possible edge cases:
        ("not_a_ferrybox/just_kidding", True),  # Whitelisting is generous
        ("ferrybox/TOftp", True),  # Blacklisting is case-sensitive
        ("ferrybox/toFTP_temp", True),  # Blacklisting is specific
    ),
)
def test_ferryboxresourceraw_validates_directory_names(
    wordless_tmp_path, given_directory_path: str, expected_match: bool
):
    # Given a directory path and an allowed filename
    given_directory_path = wordless_tmp_path / given_directory_path
    given_filename = Path("a_2026-02-12.txt")

    # When giving it to FerryboxResourceRaw
    resource = FerryboxResourceRaw.from_source_file(given_directory_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_ferryboxresourceraw_extracts_attributes_from_filename(wordless_tmp_path):
    # Given a filename made up of specific components
    given_prefix = "abc"
    given_year = "2026"
    given_month = "01"
    given_day = "02"
    given_hour = "20"
    given_minute = "30"
    given_second = "45"

    given_filename = Path(
        f"{given_prefix} {given_year}{given_month}{given_day} "
        f"{given_hour}{given_minute}{given_second}.txt"
    )

    # When giving it to FerryboxResourceRaw
    resource = FerryboxResourceRaw.from_source_file(
        wordless_tmp_path / "ferrybox", given_filename
    )

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "prefix",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
    }

    # And the values are intact
    assert resource.attributes["prefix"] == given_prefix
    assert resource.attributes["year"] == given_year
    assert resource.attributes["month"] == given_month
    assert resource.attributes["day"] == given_day
    assert resource.attributes["hour"] == given_hour
    assert resource.attributes["minute"] == given_minute
    assert resource.attributes["second"] == given_second


@pytest.mark.parametrize(
    "given_filename, expected_date",
    (
        ("Ferrybox/All_sensors_731803_2025-07-17.txt", date(2025, 7, 17)),
        ("Ferrybox/FTP_All_sensors_731803_2026-02-10_11-17.txt", date(2026, 2, 10)),
    ),
)
def test_ferryboxresourceraw_can_assemble_date(
    wordless_tmp_path, given_filename: str, expected_date
):
    # Given a filename
    # When giving it to FerryboxResourceRaw
    resource = FerryboxResourceRaw.from_source_file(
        wordless_tmp_path, Path(given_filename)
    )

    # Then the expected date is extracted from the filename
    assert resource.date == expected_date


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("2025-01-02_2025-12-31_name", True),
    ),
)
def test_ferryboxresourceco2_accepted_filenames(
    wordless_tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to FerryboxResourceCO2
    resource = FerryboxResourceCO2.from_source_file(
        wordless_tmp_path, Path(given_filename)
    )

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_ferryboxresourceco2_extracts_attributes_from_filename(wordless_tmp_path):
    # Given a filename made up of specific components
    given_from_year = "2025"
    given_from_month = "01"
    given_from_day = "02"
    given_to_year = "2026"
    given_to_month = "12"
    given_to_day = "31"
    given_name = "bob"

    given_filename = Path(
        f"{given_from_year}-{given_from_month}-{given_from_day}_"
        f"{given_to_year}-{given_to_month}-{given_to_day}_{given_name}.txt"
    )

    # When giving it to FerryboxResourceCO2
    resource = FerryboxResourceCO2.from_source_file(
        wordless_tmp_path / "ferrybox", given_filename
    )

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "from_year",
        "from_month",
        "from_day",
        "to_year",
        "to_month",
        "to_day",
        "name",
    }

    # And the values are intact
    assert resource.attributes["from_year"] == given_from_year
    assert resource.attributes["from_month"] == given_from_month
    assert resource.attributes["from_day"] == given_from_day
    assert resource.attributes["to_year"] == given_to_year
    assert resource.attributes["to_month"] == given_to_month
    assert resource.attributes["to_day"] == given_to_day
    assert resource.attributes["name"] == given_name


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("2025-01-02_2025-12-31_wiski", True),
        ("2025-01-02_2025-12-31_notwiski", False),
    ),
)
def test_ferryboxresourcewiski_accepted_filenames(
    wordless_tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".txt")

    # When giving it to FerryboxResourceWiski
    resource = FerryboxResourceWiski.from_source_file(
        wordless_tmp_path, Path(given_filename)
    )

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_ferryboxresourcewiski_extracts_attributes_from_filename(wordless_tmp_path):
    # Given a filename made up of specific components
    given_from_year = "2025"
    given_from_month = "01"
    given_from_day = "02"
    given_to_year = "2026"
    given_to_month = "12"
    given_to_day = "31"

    given_filename = Path(
        f"{given_from_year}-{given_from_month}-{given_from_day}_"
        f"{given_to_year}-{given_to_month}-{given_to_day}_wiski.txt"
    )

    # When giving it to FerryboxResourceWiski
    resource = FerryboxResourceWiski.from_source_file(
        wordless_tmp_path / "ferrybox", given_filename
    )

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "from_year",
        "from_month",
        "from_day",
        "to_year",
        "to_month",
        "to_day",
    }

    # And the values are intact
    assert resource.attributes["from_year"] == given_from_year
    assert resource.attributes["from_month"] == given_from_month
    assert resource.attributes["from_day"] == given_from_day
    assert resource.attributes["to_year"] == given_to_year
    assert resource.attributes["to_month"] == given_to_month
    assert resource.attributes["to_day"] == given_to_day
