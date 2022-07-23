from glob import glob
from pathlib import Path
from shutil import rmtree
from typing import List
import mapreduce.constants as constants


def line_count(path: str) -> int:
    return sum(1 for _ in open(path))


def dir_line_count(paths: List[str]) -> int:
    s = 0
    for path in paths:
        s += line_count(path)

    return s


def split_files(paths: List[str], file_line_limit: int, out_dir: str = constants.MAP_INPUT_DIR):
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
                    file_index += 1
                    outfile = open(f'{out_dir}/{constants.MAP_INPUT_FILE_PREFIX}-%03d.txt' % file_index, 'w')

                # write to file
                outfile.write(line)

                counter += 1


if __name__ == '__main__':
    pass
