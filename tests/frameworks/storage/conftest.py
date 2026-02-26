import shutil
import subprocess

import pytest


@pytest.fixture
def svn_repo(tmp_path):
    if not shutil.which("svnadmin"):
        pytest.skip("svnadmin not installed")

    repo_path = tmp_path / "repo"
    subprocess.run(["svnadmin", "create", str(repo_path)], check=True)
    return f"file://{repo_path}"
