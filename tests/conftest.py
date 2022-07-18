import pytest
from pathlib import Path


@pytest.fixture(scope='session')
def test_data_dir() -> Path:
    return Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def test_text_file_path(test_data_dir: Path) -> Path:
    return test_data_dir / 'file.txt'
