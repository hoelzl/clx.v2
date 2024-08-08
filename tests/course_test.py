from pathlib import Path
from tempfile import TemporaryDirectory

from clx.course import Course
from clx.file import Notebook
from clx.utils.text_utils import Text


DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "output"


def test_build_topic_map(course_spec):
    course = Course(course_spec, DATA_DIR, OUTPUT_DIR)
    course._build_topic_map()
    assert len(course._topic_map) == 3

    id1 = course._topic_map["some_topic_from_test_1"]
    assert id1.parent.name == "module_000_test_1"
    assert id1.name == "topic_100_some_topic_from_test_1"

    id2 = course._topic_map["another_topic_from_test_1"]
    assert id2.parent.name == "module_000_test_1"
    assert id2.name == "topic_110_another_topic_from_test_1"

    id3 = course._topic_map["a_topic_from_test_2"]
    assert id3.parent.name == "module_010_test_2"
    assert id3.name == "topic_100_a_topic_from_test_2"


def test_course_from_spec_sections(course_spec):
    course = Course.from_spec(course_spec, DATA_DIR, OUTPUT_DIR)
    assert len(course.sections) == 2

    section_1 = course.sections[0]
    assert len(section_1.topics) == 2
    assert section_1.name == Text(de="Woche 1", en="Week 1")

    topic_11 = section_1.topics[0]
    assert topic_11.id == "some_topic_from_test_1"
    assert topic_11.section == section_1
    assert topic_11.path.name == "topic_100_some_topic_from_test_1"

    nb1 = topic_11.notebooks[0]
    assert nb1.path.name == "slides_some_topic_from_test_1.py"
    assert isinstance(nb1, Notebook)
    assert nb1.title == Text(de="Folien von Test 1", en="Some Topic from Test 1")
    assert nb1.number_in_section == 1

    topic_12 = section_1.topics[1]
    assert topic_12.id == "a_topic_from_test_2"
    assert topic_12.section == section_1
    assert topic_12.path.name == "topic_100_a_topic_from_test_2"

    nb2 = topic_12.notebooks[0]
    assert nb2.path.name == "slides_a_topic_from_test_2.py"
    assert isinstance(nb2, Notebook)
    assert nb2.title == Text(de="Folien aus Test 2", en="A Topic from Test 2")
    assert nb2.number_in_section == 2

    section_2 = course.sections[1]
    assert len(section_2.topics) == 1

    topic_21 = section_2.topics[0]
    assert topic_21.id == "another_topic_from_test_1"
    assert topic_21.section == section_2
    assert topic_21.path.name == "topic_110_another_topic_from_test_1"


def test_course_dict_groups(course_spec):
    def src_path(dir_: str):
        return DATA_DIR / dir_

    def out_path(dir_: str):
        return OUTPUT_DIR / dir_

    course = Course.from_spec(course_spec, DATA_DIR, OUTPUT_DIR)

    assert len(course.dict_groups) == 3

    group1 = course.dict_groups[0]
    assert group1.name == Text(de="Code/Solutions", en="Code/Solutions")
    assert group1.source_dirs == (
        src_path("code/solutions/Example_1"),
        src_path("code/solutions/Example_3"),
    )
    assert group1.output_dirs("de") == (
        out_path("De/Mein Kurs/Code/Solutions/Example_1"),
        out_path("De/Mein Kurs/Code/Solutions/Example_3"),
    )
    assert group1.output_dirs("en") == (
        out_path("En/My Course/Code/Solutions/Example_1"),
        out_path("En/My Course/Code/Solutions/Example_3"),
    )

    group2 = course.dict_groups[1]
    assert group2.name == Text(de="Bonus", en="Bonus")
    assert group2.source_dirs == (src_path("div/workshops"),)
    assert group2.output_dirs("de") == (out_path("De/Mein Kurs/Bonus"),)
    assert group2.output_dirs("en") == (out_path("En/My Course/Bonus"),)

    group3 = course.dict_groups[2]
    assert group3.name == Text(de="", en="")
    assert group3.source_dirs == (src_path("root-files"),)
    assert group3.output_dirs("de") == (out_path("De/Mein Kurs"),)
    assert group3.output_dirs("en") == (out_path("En/My Course"),)


def test_course_files(course_spec):
    course = Course.from_spec(course_spec, DATA_DIR, OUTPUT_DIR)

    assert len(course.files) == 9
    assert {file.path.name for file in course.files} == {
        "my_diag.png",
        "my_diag.pu",
        "my_drawing.drawio",
        "my_drawing.png",
        "my_image.png",
        "slides_a_topic_from_test_2.py",
        "slides_another_topic_from_test_1.py",
        "slides_some_topic_from_test_1.py",
        "test.data",
    }


def test_course_notebooks(course_spec):
    course = Course.from_spec(course_spec, DATA_DIR, OUTPUT_DIR)

    assert len(course.notebooks) == 3

    nb1 = course.notebooks[0]
    assert nb1.path.name == "slides_some_topic_from_test_1.py"
    assert nb1.title == Text(de="Folien von Test 1", en="Some Topic from Test 1")
    assert nb1.number_in_section == 1

    nb2 = course.notebooks[1]
    assert nb2.path.name == "slides_a_topic_from_test_2.py"
    assert nb2.title == Text(de="Folien aus Test 2", en="A Topic from Test 2")
    assert nb2.number_in_section == 2

    nb3 = course.notebooks[2]
    assert nb3.path.name == "slides_another_topic_from_test_1.py"
    assert nb3.title == Text(
        de="Mehr Folien von Test 1", en="Another Topic from Test 1"
    )
    assert nb3.number_in_section == 1


def test_topic_matches_path(topic_1):
    # Existing slides in topic dir match
    assert topic_1.matches_path(
        topic_1.path / "slides_some_topic_from_test_1.py", False
    )
    # New slides in topic dir match
    assert topic_1.matches_path(topic_1.path / "slides_new_topic.py", False)
    # Images in the img/ subdirectory match
    assert topic_1.matches_path(topic_1.path / "img/my_image.png", False)
    # PlantUML files in the pu/ subdirectory match
    assert topic_1.matches_path(topic_1.path / "pu/my_diag.pu", False)
    # DrawIO files in the drawio/ subdirectory match
    assert topic_1.matches_path(topic_1.path / "drawio/my_drawing.drawio", False)
    # Deeply nested data files match
    assert topic_1.matches_path(topic_1.path / "data/more_data/csv/test.csv", False)

    # Files in other topics do not match
    other_topic = Path(DATA_DIR / "module_010_test_2" / "topic_200_other")
    assert not topic_1.matches_path(
        other_topic / "slides_a_topic_from_test_2.py", False
    )

    # Files in the parent module do not match
    assert not topic_1.matches_path(topic_1.path.parent / "slides_in_parent.py", False)


def test_add_file_to_course(course_spec):
    unit = Course.from_spec(course_spec, DATA_DIR, OUTPUT_DIR)
    assert len(unit.files) == 9
    topic_1 = unit.topics[0]
    topic_2 = unit.topics[1]
    # Note that we cannot easily add Notebooks, since notebooks need to actually
    # exist on disk to be added to the course, since we need information from the
    # notebook to fill out its properties.
    file_1 = topic_1.path / "python_file.py"
    assert unit.find_file(file_1) is None
    file_2 = topic_2.path / "img/my_new_image.png"
    assert unit.find_file(file_2) is None
    file_3 = topic_2.path.parent / "data/my_new_data.csv"
    assert unit.find_file(file_3) is None
    file_4 = topic_1.path / "slides_a_notebook.py"
    assert unit.find_file(file_4) is None

    unit.add_file(file_1)
    assert len(unit.files) == 10
    assert unit.find_file(file_1).path == file_1

    unit.add_file(file_2)
    assert len(unit.files) == 11
    assert unit.find_file(file_2).path == file_2

    unit.add_file(file_3)
    assert len(unit.files) == 11
    assert unit.find_file(file_3) is None

    unit.add_file(file_4)
    assert len(unit.files) == 11
    assert unit.find_file(file_4) is None


def test_course_dict_croups_copy(course_spec):
    with TemporaryDirectory() as output_dir:
        output_dir = Path(output_dir)
        course = Course.from_spec(course_spec, DATA_DIR, output_dir)
        for dict_group in course.dict_groups:
            dict_group.copy_to_output("de")
            dict_group.copy_to_output("en")

        assert len(list(output_dir.glob("**/*"))) == 28
        assert set(output_dir.glob("**/*")) == {
            output_dir / "De",
            output_dir / "De/Mein Kurs",
            output_dir / "De/Mein Kurs/Bonus",
            output_dir / "De/Mein Kurs/Bonus/Workshop-1",
            output_dir / "De/Mein Kurs/Bonus/Workshop-1/workshop-1.txt",
            output_dir / "De/Mein Kurs/Bonus/workshops-toplevel.txt",
            output_dir / "De/Mein Kurs/Code",
            output_dir / "De/Mein Kurs/Code/Solutions",
            output_dir / "De/Mein Kurs/Code/Solutions/Example_1",
            output_dir / "De/Mein Kurs/Code/Solutions/Example_1/example-1.txt",
            output_dir / "De/Mein Kurs/Code/Solutions/Example_3",
            output_dir / "De/Mein Kurs/Code/Solutions/Example_3/example-3.txt",
            output_dir / "De/Mein Kurs/root-file-1.txt",
            output_dir / "De/Mein Kurs/root-file-2",
            output_dir / "En",
            output_dir / "En/My Course",
            output_dir / "En/My Course/Bonus",
            output_dir / "En/My Course/Bonus/Workshop-1",
            output_dir / "En/My Course/Bonus/Workshop-1/workshop-1.txt",
            output_dir / "En/My Course/Bonus/workshops-toplevel.txt",
            output_dir / "En/My Course/Code",
            output_dir / "En/My Course/Code/Solutions",
            output_dir / "En/My Course/Code/Solutions/Example_1",
            output_dir / "En/My Course/Code/Solutions/Example_1/example-1.txt",
            output_dir / "En/My Course/Code/Solutions/Example_3",
            output_dir / "En/My Course/Code/Solutions/Example_3/example-3.txt",
            output_dir / "En/My Course/root-file-1.txt",
            output_dir / "En/My Course/root-file-2",
        }
