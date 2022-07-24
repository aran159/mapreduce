from glob import glob
from io import TextIOWrapper
from pathlib import Path
from typing import (
    Dict,
    List,
    Tuple
)
import mapreduce.constants as constants
import mapreduce.utils as utils


def word_list_file_to_tuple(file_handler: TextIOWrapper) -> Tuple[str]:
    return tuple(utils.tokenize(file_handler.read()))


def word_counts(words: Tuple[str]) -> Dict[str, int]:
    result = {}
    for word in words:
        if word not in result.keys():
            result[word] = 0
        result[word] += 1
    return result


def write_result(reduce_id: int, word_counts: Dict[str, int]):
    Path(constants.OUT_DIR).mkdir(exist_ok=True, parents=True)
    with open(constants.OUT_DIR + f'{constants.OUT_FILE_PREFIX}-{reduce_id}.txt', 'w') as out_file:
        for word, count in word_counts.items():
            out_file.write(word + ' ' + str(count) + '\n')


def filter_file_paths_by_reduce_id(reduce_id: int) -> List[str]:
    return glob(constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}-*-{reduce_id}.txt')


def words_in_reduce_files(reduce_id: int) -> Tuple[str]:
    words = []
    for file_path in filter_file_paths_by_reduce_id(reduce_id):
        with open(file_path, 'r') as in_file:
            words.extend(word_list_file_to_tuple(in_file))
    return tuple(words)


def reduce(reduce_id: int) -> None:
    words = words_in_reduce_files(reduce_id)
    write_result(reduce_id, word_counts(words))
