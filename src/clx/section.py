from typing import TYPE_CHECKING

from attr import Factory
from attrs import define

from clx.course_file import CourseFile, Notebook
from clx.utils.text_utils import Text

if TYPE_CHECKING:
    from clx.course import Course
    from clx.topic import Topic


@define
class Section:
    name: Text
    course: "Course"
    topics: list["Topic"] = Factory(list)

    @property
    def files(self) -> list[CourseFile]:
        return [file for topic in self.topics for file in topic.files]

    @property
    def notebooks(self) -> list[Notebook]:
        return [file for file in self.files if isinstance(file, Notebook)]

    def add_notebook_numbers(self):
        for index, nb in enumerate(self.notebooks, 1):
            nb.number_in_section = index
