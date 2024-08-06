import io

from clx.course_spec import CourseSpec, TopicSpec, parse_multilang
from clx.utils.text_utils import Text

from course_fixtures import COURSE_XML, course_xml


def test_parse_multilang(course_xml):
    assert parse_multilang(course_xml, "name") == Text(de="mein-kurs", en="my-course")
    assert parse_multilang(course_xml, "github") == Text(
        de="https://github.com/hoelzl/my-course-de",
        en="https://github.com/hoelzl/my-course-en",
    )


def test_parse_sections(course_xml):
    sections = CourseSpec.parse_sections(course_xml)
    assert len(sections) == 2
    assert sections[0].name == Text(de="woche-1", en="week-1")
    assert sections[0].topics == [
        TopicSpec(id="some_topic_from_test_1"),
        TopicSpec(id="a_topic_from_test_2"),
    ]
    assert sections[1].name == Text(de="woche-2", en="week-2")
    assert sections[1].topics == [TopicSpec("another_topic_from_test_1")]


def test_from_file():
    xml_stream = io.StringIO(COURSE_XML)
    course = CourseSpec.from_file(xml_stream)
    assert course.name == Text(de="mein-kurs", en="my-course")
    assert course.prog_lang == "python"
    assert course.description == Text(
        de="Ein Kurs über ein Thema", en="A course about a topic"
    )
    assert course.sections[0].name == Text(de="woche-1", en="week-1")
    assert course.sections[0].topics == [
        TopicSpec(id="some_topic_from_test_1"),
        TopicSpec(id="a_topic_from_test_2"),
    ]
    assert course.sections[1].name == Text(de="woche-2", en="week-2")
    assert course.sections[1].topics == [TopicSpec(id="another_topic_from_test_1")]
    assert course.github_repo == Text(
        de="https://github.com/hoelzl/my-course-de",
        en="https://github.com/hoelzl/my-course-en",
    )
