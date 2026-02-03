from datetime import datetime
from pathlib import Path

import pytest

from svea_data_manager.instruments.ifcb_classification import IFCBResourceClass

"""
^D{YEAR}{MONTH}{DAY}T{HOUR}{MINUTE}{SECOND}_{INSTRUMENT}_{PROCESS_CLASS}{VERSION}.mat$
"""


@pytest.mark.parametrize(
    "given_file_stem, expected_match",
    (
        ("x", False),
        ("very_random_file_name_refrigerator", False),
        ("D20260213T143210_IFCB_class", True),
        ("D20260213T143210_IFCB123_classABC", True),
        ("D20260213T143210_IFCB123_class", True),
        ("D20260213T143210_IFCB123_class123", True),
    ),
)
def test_ifcbresourceclass_accepted_filenames(
    tmp_path, given_file_stem, expected_match: bool
):
    # Given a file stem and an allowed suffix
    given_filename = Path(given_file_stem).with_suffix(".mat")

    # When giving it to IFCBResourceClass
    resource = IFCBResourceClass.from_source_file(tmp_path, given_filename)

    # Then the filename is matched or not according to expectation
    assert (resource is not None) == expected_match


@pytest.mark.parametrize(
    "given_filename, expected_datetime",
    (
        ("D20240101T080056_IFCB_class.mat", datetime(2024, 1, 1, 8, 0, 56)),
        ("D20250215T122010_IFCB_class.mat", datetime(2025, 2, 15, 12, 20, 10)),
        ("D20260331T204013_IFCB_class.mat", datetime(2026, 3, 31, 20, 40, 13)),
    ),
)
def test_ifcbresourceclass_can_identify_date_and_datetime(
    tmp_path, given_filename: str, expected_datetime
):
    # Given a filename
    # When giving it to IFCBResourceClass
    resource = IFCBResourceClass.from_source_file(tmp_path, Path(given_filename))

    # Then the expected date is extracted from the filename
    assert resource.date == expected_datetime.date()

    # And the expected datetime is extracted from the filename
    assert resource.datetime == expected_datetime
