from pathlib import Path

import pytest

from svea_data_manager.frameworks.exceptions import ConfigurationError
from svea_data_manager.instruments.mvp import Mvp, MvpResource


def test_mvp_raises_without_subversion_repo_url():
    # Given a configuration without target_directory.
    given_config = {"source_directory": "/any/path/"}
    assert "subversion_repo_url" not in given_config

    # When creating Mvp
    # Then it raises an exception
    with pytest.raises(ConfigurationError):
        Mvp(given_config)


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("MVP_2026-02-13_155010_ABC-123", True),
        ("aMVP_2026-02-13_155010_ABC-123", True),
        ("MVP_2026-02-13_155010_ABC123", True),
        ("bMVP_2026-02-13_155010_ABC123", True),
        # Third pattern broken:
        pytest.param("MVP_2026-02-13_155010_", True, marks=pytest.mark.xfail),
        pytest.param("cMVP_2026-02-13_155010_", True, marks=pytest.mark.xfail),
    ),
)
def test_mvpresource_accepted_filenames(
    wordless_tmp_path, given_file_stem, expected_match: bool
):
    # Given an allowed directory path
    given_directory_path = wordless_tmp_path / "SMHI_MVP"

    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".jpg")

    # When giving it to MVPResource
    resource = MvpResource.from_source_file(given_directory_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_directory_path, expected_match",
    (
        ("", False),
        ("SMHI_MVP", True),
        ("smhi_mvp", True),
        ("MVP/smhi_data", True),
        # Possible edge cases:
        ("smhi_", True),  # Double checks, "MVP" checked on both full path and filename.
        ("not_smhi_or_MVP/just_kidding", True),  # Whitelisting is generous
        ("MVP_SMHI/SMHI/MVP", False),  # Whitelisting is specific
    ),
)
def test_mvpresource_validates_directory_names(
    wordless_tmp_path, given_directory_path: str, expected_match: bool
):
    # Given a directory path and an allowed filename
    given_directory_path = wordless_tmp_path / given_directory_path
    given_filename = Path("MVP_2026-02-13_155010_ABC-123")

    # When giving it to MVPResource
    resource = MvpResource.from_source_file(given_directory_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


def test_mvpresource_extracts_attributes_from_filename(wordless_tmp_path):
    # Given a filename made up of specific components
    given_prefix = "q"
    given_instrument = "MVP"
    given_year = "2001"
    given_month = "02"
    given_day = "03"
    given_hour = "04"
    given_minute = "05"
    given_second = "06"
    given_transect = "transect"
    given_suffix = ".anysuffix"

    given_filename = Path(
        f"{given_prefix}{given_instrument}_"
        f"{given_year}-{given_month}-{given_day}_"
        f"{given_hour}{given_minute}{given_second}_"
        f"{given_transect}{given_suffix}"
    )

    given_directory_path = wordless_tmp_path / "smhi_"

    # When giving it to MVPResource
    resource = MvpResource.from_source_file(given_directory_path, given_filename)

    # Then all the components are available in resource attributes
    assert set(resource.attributes.keys()) == {
        "prefix",
        "instrument",
        "year",
        "month",
        "day",
        "hour",
        "minute",
        "second",
        "transect",
        "suffix",
    }

    # And the values are intact
    assert resource.attributes["prefix"] == given_prefix
    assert resource.attributes["instrument"] == given_instrument
    assert resource.attributes["year"] == given_year
    assert resource.attributes["month"] == given_month
    assert resource.attributes["day"] == given_day
    assert resource.attributes["hour"] == given_hour
    assert resource.attributes["minute"] == given_minute
    assert resource.attributes["second"] == given_second
    assert resource.attributes["transect"] == given_transect.upper()
    assert resource.attributes["suffix"] == given_suffix
