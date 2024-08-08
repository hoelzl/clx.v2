from pathlib import Path
from typing import cast

from clx.course_file import (
    DataFile,
    DrawIoFile,
    CourseFile,
    Notebook,
    PlantUmlFile,
)
from clx.file_ops import (
    ConvertDrawIoFile,
    ConvertPlantUmlFile,
    CopyFileOperation,
    ProcessNotebookOperation,
)
from clx.operation import Concurrently
from clx.utils.path_utils import output_specs

PLANT_UML_FILE = "pu/my_diag.pu"
DRAWIO_FILE = "drawio/my_drawing.drawio"
DATA_FILE = "data/test.data"
NOTEBOOK_FILE = "slides_some_topic_from_test_1.py"


def test_file_from_path_plant_uml(course, section_1, topic_1):
    file_path = topic_1.path / PLANT_UML_FILE
    expected_output = file_path.parents[1] / "img/my_diag.png"

    unit = CourseFile.from_path(course, file_path, topic_1)

    assert isinstance(unit, PlantUmlFile)
    assert unit.path == file_path
    assert unit.topic == topic_1
    assert unit.section == section_1
    assert unit.relative_path == Path(PLANT_UML_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


async def test_file_from_path_plant_uml_operations(course, topic_1):
    file_path = topic_1.path / PLANT_UML_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    process_op = await unit.get_processing_operation(course.output_root)
    assert isinstance(process_op, ConvertPlantUmlFile)
    assert process_op.input_file == unit
    assert process_op.output_file == topic_1.path / "img/my_diag.png"


def test_file_from_path_drawio(course, section_1, topic_1):
    file_path = topic_1.path / DRAWIO_FILE
    expected_output = file_path.parents[1] / "img/my_drawing.png"

    unit = CourseFile.from_path(course, file_path, topic_1)

    assert isinstance(unit, DrawIoFile)
    assert unit.path == file_path
    assert unit.topic == topic_1
    assert unit.section == section_1
    assert unit.relative_path == Path(DRAWIO_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset({expected_output})


async def test_file_from_path_drawio_operations(course, topic_1):
    file_path = topic_1.path / DRAWIO_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    process_op = await unit.get_processing_operation(course.output_root)
    assert isinstance(process_op, ConvertDrawIoFile)
    assert process_op.input_file == unit
    assert process_op.output_file == topic_1.path / "img/my_drawing.png"


def test_file_from_path_data_file(course, section_1, topic_1):
    file_path = topic_1.path / DATA_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    assert isinstance(unit, DataFile)
    assert unit.path == file_path
    assert unit.topic == topic_1
    assert unit.section == section_1
    assert unit.relative_path == Path("data/test.data")
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()


async def test_file_from_path_data_file_operations(course, topic_1):
    file_path = topic_1.path / DATA_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    process_op = await unit.get_processing_operation(course.output_root)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[CopyFileOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == course.output_root / (
        f"De/Mein Kurs/Folien/Html/Code-Along/Woche 1/{DATA_FILE}"
    )

    assert len(ops) == len(list(output_specs(course, course.output_root)))
    assert all(isinstance(op, CopyFileOperation) for op in ops)
    assert all(op.input_file == unit for op in ops)
    assert all(op.output_file.name == "test.data" for op in ops)
    assert all(op.output_file.parent.name == "data" for op in ops)


def test_file_from_path_notebook(course, section_1, topic_1):
    file_path = topic_1.path / NOTEBOOK_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    assert isinstance(unit, Notebook)
    assert unit.path == file_path
    assert unit.topic == topic_1
    assert unit.section == section_1
    assert unit.relative_path == Path(NOTEBOOK_FILE)
    assert unit.generated_outputs == set()
    assert unit.generated_sources == frozenset()
    assert unit.prog_lang == "python"


async def test_file_from_path_notebook_operations(course, topic_1):
    file_path = topic_1.path / NOTEBOOK_FILE

    unit = CourseFile.from_path(course, file_path, topic_1)

    process_op = await unit.get_processing_operation(course.output_root)
    assert isinstance(process_op, Concurrently)

    ops = cast(list[ProcessNotebookOperation], list(process_op.operations))
    op = ops[0]
    assert op.output_file == course.output_root / (
        "De/Mein Kurs/Folien/Html/Code-Along/Woche 1/00 Folien von Test 1.html"
    )

    assert len(ops) == len(list(output_specs(course, course.output_root)))
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


async def test_data_file_generated_outputs(course, topic_1):
    file_path = topic_1.path / DATA_FILE
    unit = CourseFile.from_path(course, file_path, topic_1)

    output_dir = course.output_root
    op = await unit.get_processing_operation(output_dir)
    await op.exec()

    assert unit.generated_sources == frozenset()
    assert unit.generated_outputs == {
        output_dir / f"De/Mein Kurs/Folien/Html/Code-Along/Woche 1/{DATA_FILE}",
        output_dir / f"De/Mein Kurs/Folien/Html/Completed/Woche 1/{DATA_FILE}",
        output_dir / f"De/Mein Kurs/Folien/Notebooks/Code-Along/Woche 1/{DATA_FILE}",
        output_dir / f"De/Mein Kurs/Folien/Notebooks/Completed/Woche 1/{DATA_FILE}",
        output_dir / f"De/Mein Kurs/Folien/Python/Completed/Woche 1/{DATA_FILE}",
        output_dir / f"En/My Course/Slides/Html/Code-Along/Week 1/{DATA_FILE}",
        output_dir / f"En/My Course/Slides/Html/Completed/Week 1/{DATA_FILE}",
        output_dir / f"En/My Course/Slides/Notebooks/Code-Along/Week 1/{DATA_FILE}",
        output_dir / f"En/My Course/Slides/Notebooks/Completed/Week 1/{DATA_FILE}",
        output_dir / f"En/My Course/Slides/Python/Completed/Week 1/{DATA_FILE}",
    }
