from glob import glob


def line_count(path: str) -> int:
    return sum(1 for _ in open(path))


def dir_line_count(dir: str) -> int:
    paths = glob(f'{dir}/*', recursive=True)

    s = 0
    for path in paths:
        s += line_count(path)

    return s


if __name__ == '__main__':
    print(dir_line_count('inputs/'))
