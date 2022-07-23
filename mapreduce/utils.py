from pathlib import Path
from shutil import rmtree
from typing import Tuple
import mapreduce.constants as constants
import re
import mapreduce.constants as constants


TOKENIZE_RE_PATTERN = r'[a-z]+'

def tokenize(input_string: str) -> Tuple[str]:
    return tuple(re.findall(TOKENIZE_RE_PATTERN, input_string.lower(), flags=re.IGNORECASE))


def remove_tmp_dir():
    try:
        rmtree(Path(constants.TMP_DIR))
    except FileNotFoundError:
        pass
