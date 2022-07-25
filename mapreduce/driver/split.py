from math import floor
from pathlib import Path
from typing import List, Tuple
import mapreduce.constants as constants


def line_count(path: str) -> int:
    return sum(1 for _ in open(path))


def dir_line_count(paths: Tuple[str]) -> int:
    s = 0
    for path in paths:
        s += line_count(path)

    return s


def split_files(paths: Tuple[str], file_lengths: Tuple[int], out_dir: str = constants.MAP_INPUT_DIR):
    Path(out_dir).mkdir(exist_ok=True, parents=True)

    file_lengths_list = list(file_lengths)
    current_file_length = file_lengths_list.pop(0)

    file_index = 0
    counter = 0
    outfile = open(f'{out_dir}/{constants.MAP_INPUT_FILE_PREFIX}-{file_index}.txt', 'w')

    for path in paths:
        with open(path) as infile:
            for line in infile.readlines():
                # Write to file
                outfile.write(line)
                counter += 1

                # Change out file if length limit is reached
                if counter == current_file_length:
                    try:
                        current_file_length = file_lengths_list.pop(0)
                    except IndexError:
                        return

                    outfile.close()
                    file_index += 1
                    outfile = open(f'{out_dir}/{constants.MAP_INPUT_FILE_PREFIX}-{file_index}.txt', 'w')

                    counter = 0


def file_lengths(total_line_count: int, desired_n_files: int) -> Tuple[int]:
    ideal_lines_per_file: float = total_line_count / desired_n_files
    min_lines_per_file: int = floor(total_line_count / desired_n_files)

    assert min_lines_per_file > 0

    rest = ideal_lines_per_file - min_lines_per_file

    result = [min_lines_per_file for _ in range(desired_n_files)]

    pending_lines = rest * desired_n_files  # Result is always an integer
    for i in range(round(pending_lines)):
        result[i] += 1

    return tuple(result)
