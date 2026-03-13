from datetime import datetime
from pathlib import Path

import pytest

from svea_data_manager.frameworks.exceptions import ConfigurationError
from svea_data_manager.instruments.ifcb import (
    Ifcb,
    IfcbResourceBlobs,
    IfcbResourceFeatures,
    IfcbResourceMultiBlob,
    IfcbResourceRaw,
)


def test_ifcb_raises_without_target_directory():
    # Given a configuration without target_directory.
    given_config = {"source_directory": "/any/path/"}
    assert "target_directory" not in given_config

    # When creating Ifcb
    # Then it raises an exception
    with pytest.raises(ConfigurationError):
        Ifcb(given_config)


@pytest.mark.parametrize(
    "given_filename, expected_datetime",
    (
        ("D20240101T080056_IFCB.adc", datetime(2024, 1, 1, 8, 0, 56)),
        ("D20250215T122010_IFCB.hdr", datetime(2025, 2, 15, 12, 20, 10)),
        ("D20260331T204013_IFCB.roi", datetime(2026, 3, 31, 20, 40, 13)),
    ),
)
def test_ifcbresourceraw_can_identify_date_and_datetime(
    tmp_path, given_filename: str, expected_datetime
):
    # Given a filename
    # When giving it to IFCBResourceRaw
    resource = IfcbResourceRaw.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_datetime.date()

    # And the expected datetime is extracted from the filename
    assert resource.datetime == expected_datetime


@pytest.mark.parametrize(
    "given_suffix, expected_match",
    (
        ("", False),
        (".csv", False),
        (".xml", False),
        (".zip", False),
        (".txt", False),
        (".adc", True),
        (".hdr", True),
        (".roi", True),
    ),
)
def test_ifcbresourceraw_accepted_suffixes(
    tmp_path, given_suffix: str, expected_match: bool
):
    # Given an allowed file stem and a suffix
    given_filename = Path("D20260213T113415_IFCB").with_suffix(given_suffix)

    # When giving it to IFCBResourceRaw
    resource = IfcbResourceRaw.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("D20260213T113415_IFCB", True),
        ("D20260213T113415_IFCB123", True),
    ),
)
def test_ifcbresourceraw_accepted_filenames(
    tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".hdr")

    # When giving it to IFCBResourceRaw
    resource = IfcbResourceRaw.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("D20260213T113415_IFCB_blobs", True),
        ("D20260213T113415_IFCB_blobsABC", True),
        ("D20260213T113415_IFCB123_blobs", True),
        ("D20260213T113415_IFCB123_blobs123", True),
    ),
)
def test_ifcbresourceblobs_accepted_filenames(
    tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".zip")

    # When giving it to IFCBResourceBlobs
    resource = IfcbResourceBlobs.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_filename, expected_datetime",
    (
        ("D20240101T080056_IFCB_blobs.zip", datetime(2024, 1, 1, 8, 0, 56)),
        ("D20250215T122010_IFCB_blobs.zip", datetime(2025, 2, 15, 12, 20, 10)),
        ("D20260331T204013_IFCB_blobs.zip", datetime(2026, 3, 31, 20, 40, 13)),
    ),
)
def test_ifcbresourceblobs_can_identify_date_and_datetime(
    tmp_path, given_filename: str, expected_datetime
):
    # Given a filename
    # When giving it to IFCBResourceBlobs
    resource = IfcbResourceBlobs.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_datetime.date()

    # And the expected datetime is extracted from the filename
    assert resource.datetime == expected_datetime


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("D20260213T113415_IFCB_fea", True),
        ("D20260213T113415_IFCB123_fea", True),
        ("D20260213T113415_IFCB_fea123", True),
        ("D20260213T113415_IFCB123_feaABC", True),
    ),
)
def test_ifcbresourcefeatures_accepted_filenames(
    tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".csv")

    # When giving it to IFCBResourceFeatures
    resource = IfcbResourceFeatures.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_filename, expected_datetime",
    (
        ("D20240101T080056_IFCB_fea.csv", datetime(2024, 1, 1, 8, 0, 56)),
        ("D20250215T122010_IFCB_fea.csv", datetime(2025, 2, 15, 12, 20, 10)),
        ("D20260331T204013_IFCB_fea.csv", datetime(2026, 3, 31, 20, 40, 13)),
    ),
)
def test_ifcbresourcefeatures_can_identify_date_and_datetime(
    tmp_path, given_filename: str, expected_datetime
):
    # Given a filename
    # When giving it to IFCBResourceFeatures
    resource = IfcbResourceFeatures.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_datetime.date()

    # And the expected datetime is extracted from the filename
    assert resource.datetime == expected_datetime


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("D20260213T113415_IFCB_multiblob", True),
        ("D20260213T113415_IFCB_multiblobABC", True),
        ("D20260213T113415_IFCB123_multiblob", True),
        ("D20260213T113415_IFCB123_multiblob123", True),
    ),
)
def test_ifcbresourcemultiblob_accepted_filenames(
    tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".csv")

    # When giving it to IFCBResourceMultiBlob
    resource = IfcbResourceMultiBlob.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_filename, expected_datetime",
    (
        ("D20240101T080056_IFCB_multiblob.csv", datetime(2024, 1, 1, 8, 0, 56)),
        ("D20250215T122010_IFCB_multiblob.csv", datetime(2025, 2, 15, 12, 20, 10)),
        ("D20260331T204013_IFCB_multiblob.csv", datetime(2026, 3, 31, 20, 40, 13)),
    ),
)
def test_ifcbresourcemultiblob_can_identify_date_and_datetime(
    tmp_path, given_filename: str, expected_datetime
):
    # Given a filename
    # When giving it to IFCBResourceMultiBlob
    resource = IfcbResourceMultiBlob.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_datetime.date()

    # And the expected datetime is extracted from the filename
    assert resource.datetime == expected_datetime


def test_ifcb_adds_file_during_transform(dir_factory):
    # Given input and output directories
    given_read_dir = dir_factory("read")
    given_write_dir = dir_factory("write")

    # Given there is a .hdr file
    given_file = given_read_dir / "D20260213T113415_IFCB.hdr"
    given_file.write_text("""gpsLatitude: 56 00.00 N\ngpsLongitude: 15 00.00 E""")

    # Given IFCB
    given_config = {
        "source_directory": str(given_read_dir),
        "target_directory": str(given_write_dir),
    }

    given_ifcb = Ifcb(given_config)
    given_ifcb.read_packages()

    # When transforming package
    given_ifcb.transform_packages()

    # Given there is no corresponding txt file
    metadata_path = (
        given_write_dir / "IFCB" / "data" / "2026" / "D20260213" / given_file.name
    ).with_suffix(".txt")
    assert not metadata_path.exists()

    # Blaha
    given_ifcb.write_packages()

    # Then a new file is created
    assert metadata_path.exists()
