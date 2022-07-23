from pathlib import Path
from typing import (
    List,
    Dict,
    Tuple
)
import mapreduce.constants as constants
import mapreduce.utils as utils


def char_alphabet_position(character: str) -> int:
    assert len(character) == 1
    character = character.lower()
    return ord(character)


def partition_words(words: List[str], M: int) -> Dict[int, Tuple[str]]:
    # Initialize result
    result = {
        i: []
        for i in range(M)
    }

    for word in words:
        result[char_alphabet_position(word[0]) % M].append(word)

    return {
        partition: tuple(words)
        for partition, words in result.items()
    }


def write_partition(map_id: int, reduce_id: int, words: Tuple[str]):
    Path(constants.REDUCE_INPUT_DIR).mkdir(exist_ok=True, parents=True)
    with open(constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}_{map_id}_{reduce_id}.txt', 'w') as out_file:
        for word in words:
            out_file.write(word + '\n')



def map(map_id: int, M: int) -> None:
    with open(constants.MAP_INPUT_DIR + f'{constants.MAP_INPUT_FILE_PREFIX}_{map_id}', 'r') as in_file:
        words = utils.tokenize(in_file.read())

    for reduce_id, words_in_partition in partition_words(words, M).items():
        write_partition(map_id, reduce_id, words_in_partition)
