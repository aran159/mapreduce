from pathlib import Path
from typing import Iterable

from pytest import fixture
from mapreduce.driver.split import line_count
from mapreduce.worker.reduce import (
    filter_file_paths_by_reduce_id,
    word_list_file_to_tuple,
    word_counts,
    write_result,
    words_in_reduce_files
)
from mapreduce import constants


REDUCE_TEST_FILENAME = 'mr-0-0.txt'


def test_word_list_file_to_tuple(test_data_dir: Path):
    with open(test_data_dir / 'reduce_test' / REDUCE_TEST_FILENAME, 'r') as f:
        result = word_list_file_to_tuple(f)
        assert isinstance(result, tuple)
        assert len(result) == 6


def test_word_counts(test_data_dir: Path):
    with open(test_data_dir / 'reduce_test' / REDUCE_TEST_FILENAME, 'r') as f:
        assert word_counts(word_list_file_to_tuple(f)) == {'apple': 2, 'banana': 2, 'bike': 1, 'car': 1}


def test_write_result(remove_tmp_dir):
    test_input = {'apple': 2, 'banana': 2, 'bike': 1, 'car': 1}
    write_result(0, test_input)

    expected_out_path = constants.OUT_DIR + f'{constants.OUT_FILE_PREFIX}-{0}.txt'
    assert Path(expected_out_path).exists()
    assert line_count(expected_out_path) == len(test_input.keys())


def test_filter_file_paths_by_reduce_id(remove_tmp_dir):
    M = 100
    N = 20

    # Create test files
    Path(constants.REDUCE_INPUT_DIR).mkdir(parents=True)
    for map_id in range(N):
        for reduce_id in range(M):
            Path(constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}-{map_id}-{reduce_id}.txt').touch()

    # Test filter
    for reduce_id in range(M):
        filtered_paths = filter_file_paths_by_reduce_id(reduce_id)
        expected_filtered_paths = [
            constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}-{map_id}-{reduce_id}.txt'
            for map_id in range(N)
        ]
        assert iterables_contain_same_elements(filtered_paths, expected_filtered_paths)


def test_words_in_reduce_files(set_test_dir_as_reduce_input_dir):
    result = words_in_reduce_files(0)
    assert isinstance(result, tuple)
    assert len(result) == 7

    result = words_in_reduce_files(1)
    assert isinstance(result, tuple)
    assert len(result) == 1

    result = words_in_reduce_files(2)
    assert isinstance(result, tuple)
    assert len(result) == 0


def iterables_contain_same_elements(iterable1: Iterable, iterable2: Iterable) -> bool:
    return set(iterable2) == set(iterable1)


@fixture
def set_test_dir_as_reduce_input_dir(test_data_dir: str):
    constants.REDUCE_INPUT_DIR = str(test_data_dir / 'reduce_test') + '/'