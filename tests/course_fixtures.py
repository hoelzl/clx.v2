import io
from xml.etree import ElementTree as ETree

import pytest

COURSE_XML = """
<course>
    <github>
        <de>https://github.com/hoelzl/my-course-de</de>
        <en>https://github.com/hoelzl/my-course-en</en>
    </github>
    <name>
        <de>mein-kurs</de>
        <en>my-course</en>
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
                <de>woche-1</de>
                <en>week-1</en>
            </name>
            <topics>
                <topic>some_topic_from_test_1</topic>
                <topic>a_topic_from_test_2</topic>
            </topics>
        </section>
        <section>
            <name>
                <de>woche-2</de>
                <en>week-2</en>
            </name>
            <topics>
                <topic>another_topic_from_test_1</topic>
            </topics>
        </section>
    </sections>
</course>
"""


@pytest.fixture
def course_xml():
    return ETree.fromstring(COURSE_XML)


@pytest.fixture
def course_spec():
    from clx.course_spec import CourseSpec
    xml_stream = io.StringIO(COURSE_XML)

    return CourseSpec.from_file(xml_stream)