import uuid

import pytest


@pytest.fixture
def wordless_tmp_path(tmp_path_factory):
    """Create a temporary directory without a meaningful name

    The builtin tmp_path inserts the test function name in the path."""
    return tmp_path_factory.mktemp(uuid.uuid4().hex[:8])
