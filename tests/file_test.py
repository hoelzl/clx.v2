from pathlib import Path
from typing import cast

from tests.course_fixtures import course, course_spec  # noqa

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
from tests.course_fixtures import COURSE, DATA_DIR, OUTPUT_DIR

TOPIC_1_DIR = DATA_DIR / "slides/module_000_test_1/topic_100_some_topic_from_test_1"
SECTION = Section(name=Text(de="Woche 1", en="Week 1"), course=COURSE)
TOPIC_1 = Topic(id="some_topic", section=SECTION, path=TOPIC_1_DIR)

PLANT_UML_FILE = "pu/my_diag.pu"
DRAWIO_FILE = "drawio/my_drawing.drawio"
DATA_FILE = "data/test.data"
NOTEBOOK_FILE = "slides_some_topic_from_test_1.py"


def test_file_from_path_plant_uml(course):
    file_path = TOPIC_1_DIR / PLANT_UML_FILE
    expected_output = file_path.parents[1] / "img/my_diag.png"

    unit = File.from_path(course, file_path, TOPIC_1)

    assert isinstance(unit, PlantUmlFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(PLANT_UML_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


def test_file_from_path_plant_uml_operations(course):
    file_path = TOPIC_1_DIR / PLANT_UML_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    process_op = unit.get_processing_operation(OUTPUT_DIR)
    assert isinstance(process_op, ConvertPlantUmlFile)
    assert process_op.input_file == unit
    assert process_op.output_file == TOPIC_1_DIR / "img/my_diag.png"


def test_file_from_path_drawio(course):
    file_path = TOPIC_1_DIR / DRAWIO_FILE
    expected_output = file_path.parents[1] / "img/my_drawing.png"

    unit = File.from_path(course, file_path, TOPIC_1)

    assert isinstance(unit, DrawIoFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(DRAWIO_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


def test_file_from_path_drawio_operations(course):
    file_path = TOPIC_1_DIR / DRAWIO_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    process_op = unit.get_processing_operation(OUTPUT_DIR)
    assert isinstance(process_op, ConvertDrawIoFile)
    assert process_op.input_file == unit
    assert process_op.output_file == TOPIC_1_DIR / "img/my_drawing.png"


def test_file_from_path_data_file(course):
    file_path = TOPIC_1_DIR / DATA_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    assert isinstance(unit, DataFile)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path("data/test.data")
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()
    assert unit.delete_op() == NoOperation()


def test_file_from_path_data_file_operations(course):
    file_path = TOPIC_1_DIR / DATA_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    process_op = unit.get_processing_operation(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[CopyFileOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / (
        f"De/Mein Kurs/Html/Code-Along/Woche 1/{DATA_FILE}"
    )

    assert len(ops) == len(list(output_specs(course, OUTPUT_DIR)))
    assert all(isinstance(op, CopyFileOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(op.output_file.name == "test.data" for op in ops)
    assert all(op.output_file.parent.name == "data" for op in ops)


def test_file_from_path_notebook(course):
    file_path = TOPIC_1_DIR / NOTEBOOK_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    assert isinstance(unit, Notebook)
    assert unit.path == file_path
    assert unit.topic == TOPIC_1
    assert unit.section == SECTION
    assert unit.relative_path == Path(NOTEBOOK_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()


def test_file_from_path_notebook_operations(course):
    file_path = TOPIC_1_DIR / NOTEBOOK_FILE

    unit = File.from_path(course, file_path, TOPIC_1)

    assert unit.delete_op() == NoOperation()

    process_op = unit.get_processing_operation(OUTPUT_DIR)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[ProcessNotebookOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == OUTPUT_DIR / (
        "De/Mein Kurs/Html/Code-Along/Woche 1/00 Folien von " "Test 1.html"
    )

    assert len(ops) == len(list(output_specs(course, OUTPUT_DIR)))
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


async def test_data_file_generated_outputs(course):
    file_path = TOPIC_1_DIR / DATA_FILE
    unit = File.from_path(course, file_path, TOPIC_1)

    await unit.get_processing_operation(OUTPUT_DIR).exec()

    assert unit.generated_sources == frozenset()
    assert unit.generated_outputs == {
        OUTPUT_DIR / f"De/Mein Kurs/Html/Code-Along/Woche 1/{DATA_FILE}",
        OUTPUT_DIR / f"De/Mein Kurs/Html/Completed/Woche 1/{DATA_FILE}",
        OUTPUT_DIR / f"De/Mein Kurs/Notebooks/Code-Along/Woche 1/{DATA_FILE}",
        OUTPUT_DIR / f"De/Mein Kurs/Notebooks/Completed/Woche 1/{DATA_FILE}",
        OUTPUT_DIR / f"De/Mein Kurs/Python/Completed/Woche 1/{DATA_FILE}",
        OUTPUT_DIR / f"En/My Course/Html/Code-Along/Week 1/{DATA_FILE}",
        OUTPUT_DIR / f"En/My Course/Html/Completed/Week 1/{DATA_FILE}",
        OUTPUT_DIR / f"En/My Course/Notebooks/Code-Along/Week 1/{DATA_FILE}",
        OUTPUT_DIR / f"En/My Course/Notebooks/Completed/Week 1/{DATA_FILE}",
        OUTPUT_DIR / f"En/My Course/Python/Completed/Week 1/{DATA_FILE}",
    }
