from pathlib import Path
from shutil import rmtree
import pytest
from mapreduce.worker.map import (
    char_alphabet_position,
    partition_words,
    write_partition,
)
import string
from mapreduce import constants


def test_char_alphabet_position():
    for expected_position, character in enumerate(string.ascii_lowercase):
        assert char_alphabet_position(character) == expected_position + 97
    for expected_position, character in enumerate(string.ascii_uppercase):
        assert char_alphabet_position(character) == expected_position + 97

    assert char_alphabet_position('ñ') == 241
    assert char_alphabet_position('Ñ') == 241

    with pytest.raises(AssertionError):
        char_alphabet_position('abcd')


def test_partition_words():
    LIST_OF_WORDS = ['app', 'building', 'car', 'dance', 'building']
    assert partition_words(LIST_OF_WORDS, 1) == {0: ('app', 'building', 'car', 'dance', 'building')}
    assert partition_words(LIST_OF_WORDS, 2) == {0: ('building', 'dance', 'building'), 1: ('app', 'car')}
    assert partition_words(LIST_OF_WORDS, 3) == {0: ('car',), 1: ('app', 'dance'), 2: ('building', 'building')}
    assert partition_words(LIST_OF_WORDS, 4) == {0: ('dance',), 1: ('app',), 2: ('building', 'building'), 3: ('car',)}
    assert partition_words(LIST_OF_WORDS, 5) == {0: ('dance',), 1: (), 2: ('app',), 3: ('building', 'building'), 4: ('car',)}


def test_write_partition(remove_tmp_dir):
    for map_id, reduce_id in (
        (0, 0),
        (0, 1),
        (1, 1)
    ):
        write_partition(map_id, reduce_id, ('my', 'car'))
        expected_out_path = constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}-{map_id}-{reduce_id}.txt'
        Path(expected_out_path).exists()
        with open(expected_out_path, 'r') as f:
            assert f.read() == 'my\ncar\n'

