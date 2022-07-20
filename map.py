from pathlib import Path
from typing import (
    List,
    Dict,
    Tuple
)
import re
import constants


pattern = '[a-z]+'


def tokenize(input_string: str) -> List[str]:
    return re.findall(pattern, input_string.lower(), flags=re.IGNORECASE)


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
    with open(constants.REDUCE_INPUT_DIR + f'{constants.REDUCE_INPUT_FILE_PREFIX}_{map_id}_{reduce_id}', 'w') as out_file:
        for word in words:
            out_file.write(word + '\n')



def map(in_path: str, map_id: int, M: int) -> None:
    with open(in_path, 'r') as in_file:
        words = tokenize(in_file.read())

    for reduce_id, words_in_partition in partition_words(words, M).items():
        write_partition(map_id, reduce_id, words_in_partition)
