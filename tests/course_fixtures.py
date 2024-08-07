import io
from pathlib import Path
from xml.etree import ElementTree as ETree

import pytest

from clx.course import Course
from clx.course_spec import CourseSpec

COURSE_XML = """
<course>
    <github>
        <de>https://github.com/hoelzl/my-course-de</de>
        <en>https://github.com/hoelzl/my-course-en</en>
    </github>
    <name>
        <de>Mein Kurs</de>
        <en>My Course</en>
    </name>
    <prog-lang>python</prog-lang>
    <description>
        <de>Ein Kurs über ein Thema</de>
        <en>A course about a topic</en>
    </description>
    <certificate>
        <de>...</de>
        <en>...</en>
    </certificate>
    <sections>
        <section>
            <name>
                <de>Woche 1</de>
                <en>Week 1</en>
            </name>
            <topics>
                <topic>some_topic_from_test_1</topic>
                <topic>a_topic_from_test_2</topic>
            </topics>
        </section>
        <section>
            <name>
                <de>Woche 2</de>
                <en>Week 2</en>
            </name>
            <topics>
                <topic>another_topic_from_test_1</topic>
            </topics>
        </section>
    </sections>
</course>
"""
DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path("/output")
COURSE_SPEC_STREAM = io.StringIO(COURSE_XML)
COURSE_SPEC = CourseSpec.from_file(COURSE_SPEC_STREAM)
COURSE = Course(COURSE_SPEC, DATA_DIR, OUTPUT_DIR)


@pytest.fixture
def course_xml():
    return ETree.fromstring(COURSE_XML)


@pytest.fixture
def course_spec():
    from clx.course_spec import CourseSpec
    xml_stream = io.StringIO(COURSE_XML)

    return CourseSpec.from_file(xml_stream)


@pytest.fixture
def course(course_spec):
    return Course(course_spec, DATA_DIR, OUTPUT_DIR)