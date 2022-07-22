import pytest
from pathlib import Path
from shutil import rmtree
from split import (
    line_count,
    dir_line_count,
    split_files,
)
from glob import glob
import constants


@pytest.fixture
def split_test_data_dir(test_data_dir: Path):
    return test_data_dir / 'split'


def test_line_count(test_text_file_path: Path) -> None:
    assert line_count(str(test_text_file_path)) == 6


def test_dir_line_count(split_test_data_dir: Path) -> None:
    assert dir_line_count(glob(f'{split_test_data_dir}/*', recursive=True)) == 7


@pytest.mark.parametrize(
    "max_line_number, expected_length",
    [
        (1, [1 for _ in range(7)]),
        (2, [1, 2, 2, 2]),
        (3, [1, 3, 3]),
        (4, [3, 4]),
        (5, [2, 5]),
        (6, [1, 6]),
        (7, [7, ]),
        (8, [7, ]),
    ]
)
def test_split_returns_expected_length_files(max_line_number, expected_length, split_test_data_dir: Path, remove_tmp_dir) -> None:
    split_files(glob(f'{split_test_data_dir}/*', recursive=True), max_line_number)

    for path, expected_length in zip(glob(f'{constants.MAP_INPUT_DIR}/*'), expected_length):
        assert line_count(path) == expected_length


def test_split_raises_value_error(split_test_data_dir: Path) -> None:
    for invalid_argument in (-1, 0):
        with pytest.raises(ValueError):
            split_files(glob(f'{split_test_data_dir}/*', recursive=True), invalid_argument)
