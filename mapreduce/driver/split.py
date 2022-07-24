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


def split_files(paths: Tuple[str], file_line_limit: int, out_dir: str = constants.MAP_INPUT_DIR):
    if not file_line_limit > 0:
        raise ValueError('file_line_limit must be a positive number')

    Path(out_dir).mkdir(exist_ok=True, parents=True)

    file_index = 0
    outfile = None
    counter = 0
    for path in paths:
        with open(path) as infile:
            for line in infile.readlines():
                if counter % file_line_limit == 0:
                    # close old file
                    if outfile is not None:
                        outfile.close()
                    # create new file
                    outfile = open(f'{out_dir}/{constants.MAP_INPUT_FILE_PREFIX}-{file_index}.txt', 'w')
                    file_index += 1

                # write to file
                outfile.write(line)

                counter += 1
