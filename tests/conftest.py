import pathlib
import pytest


@pytest.fixture
def project_root(tmp_path: pathlib.Path) -> pathlib.Path:
    (tmp_path / "programs").mkdir()
    (tmp_path / "data").mkdir()
    (tmp_path / "logs").mkdir()
    return tmp_path
