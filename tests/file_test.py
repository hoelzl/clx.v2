from pathlib import Path
from typing import cast

from clx.course import Section, Topic
from clx.file import (
    CopyFileOperation,
    DataFile,
    File,
    ProcessNotebookOperation,
)
from clx.operation import Concurrently, NoOperation
from clx.utils.path_utils import output_specs
from clx.utils.text_utils import Text

DATA_DIR = Path(__file__).parent / "data"
TOPIC_1_DIR = DATA_DIR / "slides/module_000_test_1/topic_100_some_topic_from_test_1"
SECTION = Section(name=Text(de="woche-1", en="week-1"))
TOPIC_1 = Topic(id="some_topic", section=SECTION, path=TOPIC_1_DIR)
OUTPUT_DIR = Path("/output")


def test_file_from_path_data_file():
    file_path = TOPIC_1_DIR / "data/test.data"

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, DataFile)
    assert unit.path == file_path
    assert unit.delete_op() == NoOperation()

    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[CopyFileOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / "De/Html/Code-Along/woche-1/data/test.data"

    assert len(ops) == len(list(output_specs(OUTPUT_DIR)))
    assert all(isinstance(op, CopyFileOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(op.output_file.name == "test.data" for op in ops)
    assert all(op.output_file.parent.name == "data" for op in ops)


def test_file_from_path_notebook():
    file_path = TOPIC_1_DIR / "slides_some_topic_from_test_1.py"

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, File)
    assert unit.path == file_path
    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)
    assert unit.delete_op() == NoOperation()

    ops = cast(list[ProcessNotebookOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / ("De/Html/Code-Along/woche-1/00 Folien von "
                                           "Test 1.html")

    assert len(ops) == len(list(output_specs(OUTPUT_DIR)))
    assert all(isinstance(op, ProcessNotebookOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(
        op.output_file.stem == "00 Folien von Test 1"
        for op in ops
        if op.lang == "de"
    )
    assert all(
        op.output_file.stem == f"00 Some Topic from Test 1"
        for op in ops
        if op.lang == "en"
    )
