from pathlib import Path
from typing import cast

from clx.course import Section, Topic
from clx.file import (
    ConvertDrawIoFile,
    ConvertPlantUmlFile,
    CopyFileOperation,
    DataFile,
    DrawIoFile,
    File,
    Notebook,
    PlantUmlFile,
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
PLANT_UML_FILE = "pu/my_diag.pu"
DRAWIO_FILE = "drawio/my_drawing.drawio"
DATA_FILE = "data/test.data"
NOTEBOOK_FILE = "slides_some_topic_from_test_1.py"


def test_file_from_path_plant_uml():
    file_path = TOPIC_1_DIR / PLANT_UML_FILE
    expected_output = file_path.parents[1] / "img/my_diag.png"

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, PlantUmlFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(PLANT_UML_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


def test_file_from_path_plant_uml_operations():
    file_path = TOPIC_1_DIR / PLANT_UML_FILE

    unit = File.from_path(file_path, TOPIC_1)

    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, ConvertPlantUmlFile)
    assert process_op.input_file == unit
    assert process_op.output_file == TOPIC_1_DIR / "img/my_diag.png"


def test_file_from_path_drawio():
    file_path = TOPIC_1_DIR / DRAWIO_FILE
    expected_output = file_path.parents[1] / "img/my_drawing.png"

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, DrawIoFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(DRAWIO_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


def test_file_from_path_drawio_operations():
    file_path = TOPIC_1_DIR / DRAWIO_FILE

    unit = File.from_path(file_path, TOPIC_1)

    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, ConvertDrawIoFile)
    assert process_op.input_file == unit
    assert process_op.output_file == TOPIC_1_DIR / "img/my_drawing.png"


def test_file_from_path_data_file():
    file_path = TOPIC_1_DIR / DATA_FILE

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, DataFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path("data/test.data")
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()
    assert unit.delete_op() == NoOperation()


def test_file_from_path_data_file_operations():
    file_path = TOPIC_1_DIR / DATA_FILE

    unit = File.from_path(file_path, TOPIC_1)

    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[CopyFileOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / f"De/Html/Code-Along/woche-1/{DATA_FILE}"

    assert len(ops) == len(list(output_specs(OUTPUT_DIR)))
    assert all(isinstance(op, CopyFileOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(op.output_file.name == "test.data" for op in ops)
    assert all(op.output_file.parent.name == "data" for op in ops)


def test_file_from_path_notebook():
    file_path = TOPIC_1_DIR / NOTEBOOK_FILE

    unit = File.from_path(file_path, TOPIC_1)

    assert isinstance(unit, Notebook)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(NOTEBOOK_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()


def test_file_from_path_notebook_operations():
    file_path = TOPIC_1_DIR / NOTEBOOK_FILE

    unit = File.from_path(file_path, TOPIC_1)

    assert unit.delete_op() == NoOperation()

    process_op = unit.process_op(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[ProcessNotebookOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / (
        "De/Html/Code-Along/woche-1/00 Folien von " "Test 1.html"
    )

    assert len(ops) == len(list(output_specs(OUTPUT_DIR)))
    assert all(isinstance(op, ProcessNotebookOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(
        op.output_file.stem == "00 Folien von Test 1" for op in ops if op.lang == "de"
    )
    assert all(
        op.output_file.stem == f"00 Some Topic from Test 1"
        for op in ops
        if op.lang == "en"
    )


async def test_data_file_generated_outputs():
    file_path = TOPIC_1_DIR / DATA_FILE
    unit = File.from_path(file_path, TOPIC_1)

    await unit.process_op(OUTPUT_DIR).exec()

    assert unit.generated_sources == frozenset()