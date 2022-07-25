import pytest
from pathlib import Path
from mapreduce.driver.split import (
    line_count,
    dir_line_count,
    split_files,
    file_lengths,
)
from glob import glob
from mapreduce import constants


@pytest.fixture
def split_test_data_dir(test_data_dir: Path):
    return test_data_dir / 'split'


def test_line_count(test_text_file_path: Path) -> None:
    assert line_count(str(test_text_file_path)) == 6


def test_dir_line_count(split_test_data_dir: Path) -> None:
    assert dir_line_count(glob(f'{split_test_data_dir}/*', recursive=True)) == 7


@pytest.mark.parametrize(
    "input_, expected_lengths",
    [
        ((7, ), (7, )),
        ((4, 3), (4, 3)),
        ((3, 2, 2), (3, 2, 2)),
        ((2, 2, 2, 1), (2, 2, 2, 1)),
        ((2, 2, 1, 1, 1), (2, 2, 1, 1, 1)),
        ((2, 1, 1, 1, 1, 1), (2, 1, 1, 1, 1, 1)),
        ((1, 1, 1, 1, 1, 1, 1), (1, 1, 1, 1, 1, 1, 1)),
    ]
)
def test_split_returns_expected_length_files(input_, expected_lengths, split_test_data_dir: Path, remove_tmp_dir) -> None:
    split_files(glob(f'{split_test_data_dir}/*', recursive=True), input_)

    paths = glob(f'{constants.MAP_INPUT_DIR}/*')
    paths.sort()
    for path, expected_lengths in zip(paths, expected_lengths):
        assert line_count(path) == expected_lengths


@pytest.mark.parametrize(
    "input_, expected",
    [
        ((7, 1), (7, )),
        ((7, 2), (4, 3)),
        ((7, 3), (3, 2, 2)),
        ((7, 4), (2, 2, 2, 1)),
        ((7, 5), (2, 2, 1, 1, 1)),
        ((7, 6), (2, 1, 1, 1, 1, 1)),
        ((7, 7), (1, 1, 1, 1, 1, 1, 1)),
    ]
)
def test_file_lengths(input_, expected):
    assert file_lengths(*input_) == expected


def test_file_lengths_raises_assertion_error():
    with pytest.raises(AssertionError):
        file_lengths(7, 8)