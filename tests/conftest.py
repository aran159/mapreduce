import pytest
from pathlib import Path
from mapreduce import utils


@pytest.fixture(scope='session')
def test_data_dir() -> Path:
    return Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def test_text_file_path(test_data_dir: Path) -> Path:
    return test_data_dir / 'split' / 'file.txt'


@pytest.fixture
def remove_tmp_dir():
    utils.remove_tmp_dir()
