from pathlib import Path
from shutil import rmtree
from reduce import (
    input_to_tuple,
    word_counts,
    write_result,
)
import constants


REDUCE_TEST_FILENAME = 'reduce-test.txt'


def test_input_to_tuple(test_data_dir: Path):
    with open(test_data_dir / REDUCE_TEST_FILENAME, 'r') as f:
        result = input_to_tuple(f)
        assert isinstance(result, tuple)
        assert len(result) == 6


def test_input_to_tuple(test_data_dir: Path):
    with open(test_data_dir / REDUCE_TEST_FILENAME, 'r') as f:
        assert word_counts(input_to_tuple(f)) == {'apple': 2, 'banana': 2, 'bike': 1, 'car': 1}


def test_write_result():
    try:
        rmtree(Path(constants.TMP_DIR))
    except FileNotFoundError:
        pass

    write_result(0, {'apple': 2, 'banana': 2, 'bike': 1, 'car': 1})
