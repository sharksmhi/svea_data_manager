from datetime import datetime
from pathlib import Path

import pytest

from svea_data_manager.instruments.ifcb import (
    IfcbResourceBlobs,
    IfcbResourceFeatures,
    IfcbResourceMultiBlob,
    IfcbResourceRaw,
)


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
