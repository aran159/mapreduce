from pathlib import Path
from split import (
    line_count,
    dir_line_count,
)


def test_line_count(test_text_file_path: Path) -> None:
    assert line_count(str(test_text_file_path)) == 6


def test_dir_line_count(test_data_dir: Path) -> None:
    assert dir_line_count(str(test_data_dir)) == 7
