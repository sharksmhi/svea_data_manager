from pathlib import Path

import pytest


@pytest.fixture
def dir_factory(tmp_path):
    def _dir_factory(name: str | Path):
        new_dir = tmp_path / name
        new_dir.mkdir(parents=True)
        return new_dir

    return _dir_factory
