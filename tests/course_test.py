from pathlib import Path

from clx.course import Course
from clx.file import Notebook
from clx.utils.text_utils import Text
from course_fixtures import course_spec


def test_build_topic_map(course_spec):
    course = Course(course_spec, Path(__file__).parent / "data")
    course.build_topic_map()
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


def test_course_from_spec(course_spec):
    course = Course.from_spec(course_spec, Path(__file__).parent / "data")  # noqa
    assert len(course.sections) == 2

    section_1 = course.sections[0]
    assert len(section_1.topics) == 2
    assert section_1.name == Text(de="woche-1", en="week-1")

    topic_11 = section_1.topics[0]
    assert topic_11.id == "some_topic_from_test_1"
    assert topic_11.section == section_1
    assert topic_11.path.name == "topic_100_some_topic_from_test_1"

    nb1 = topic_11.files[0]
    assert nb1.path.name == "slides_some_topic_from_test_1.py"
    assert isinstance(nb1, Notebook)
    assert nb1.title == Text(de="Folien von Test 1", en="Some Topic from Test 1")
    assert nb1.number_in_section == 1

    topic_12 = section_1.topics[1]
    assert topic_12.id == "a_topic_from_test_2"
    assert topic_12.section == section_1
    assert topic_12.path.name == "topic_100_a_topic_from_test_2"

    nb2 = topic_12.files[0]
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
