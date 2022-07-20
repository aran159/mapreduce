from io import TextIOWrapper
from pathlib import Path
from typing import (
    Dict,
    Tuple
)
import constants


def input_to_tuple(file_handler: TextIOWrapper) -> Tuple[str]:
    return tuple(file_handler.read().strip().split('\n'))


def word_counts(words: Tuple[str]) -> Dict[str, int]:
    result = {}
    for word in words:
        if word not in result.keys():
            result[word] = 0
        result[word] += 1
    return result


def write_result(reduce_id: int, word_counts: Dict[str, int]):
    Path(constants.OUT_DIR).mkdir(exist_ok=True, parents=True)
    with open(constants.OUT_DIR + f'{constants.OUT_FILE_PREFIX}-{reduce_id}', 'w') as out_file:
        for word, count in word_counts.items():
            out_file.write(word + ' ' + str(count) + '\n')


def reduce(map_id: int, reduce_id: int) -> None:
    with open(constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}_{map_id}_{reduce_id}', 'r') as in_file:
        words = input_to_tuple(in_file)

    write_result(reduce_id, word_counts(words))
