from pathlib import Path

from tests.course_fixtures import course, course_spec # noqa

from clx.utils.path_utils import Format, Lang, Mode, is_slides_file, output_specs


def test_is_slides_file():
    assert is_slides_file(Path("slides_1.py"))
    assert is_slides_file(Path("slides_2.cpp"))
    assert is_slides_file(Path("slides_3.md"))
    assert not is_slides_file(Path("slides4.py"))
    assert not is_slides_file(Path("test.py"))


def test_output_spec(course):
    unit = list(output_specs(course, Path("slides_1.py")))
    assert len(unit) == 10

    # Half the outputs should be in each language.
    assert len([os for os in unit if os.lang == Lang.DE]) == 5
    assert len([os for os in unit if os.lang == Lang.EN]) == 5

    # We generate HTML and notebook files for each language and mode.
    # Code files are only generated for completed mode.
    assert len([os for os in unit if os.format == Format.HTML]) == 4
    assert len([os for os in unit if os.format == Format.NOTEBOOK]) == 4
    assert len([os for os in unit if os.format == Format.CODE]) == 2
    assert len([os for os in unit if os.mode == Mode.CODE_ALONG]) == 4
    assert len([os for os in unit if os.mode == Mode.COMPLETED]) == 6

    os1 = unit[0]
    assert os1.lang == Lang.DE
    assert os1.format == Format.HTML
    assert os1.mode == Mode.CODE_ALONG
