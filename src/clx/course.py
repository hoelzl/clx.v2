import logging
from pathlib import Path

from attrs import define, frozen, Factory
from clx.course_spec import CourseSpec
from clx.file import File, Notebook
from clx.utils.path_utils import simplify_ordered_name
from clx.utils.text_utils import Text


@frozen
class Topic:
    id: str
    section: "Section"
    path: Path
    files: list[File] = Factory(list)

    def __attrs_post_init__(self):
        self.build_files()

    def build_files(self):
        for file in sorted(list(self.path.iterdir())):
            if file.is_file():
                self.files.append(File.from_path(file, self))


@define
class Section:
    name: Text
    topics: list[Topic] = Factory(list)

    @property
    def files(self) -> list[File]:
        return [file for topic in self.topics for file in topic.files]

    @property
    def notebooks(self) -> list[Notebook]:
        return [file for file in self.files if isinstance(file, Notebook)]

    def add_notebook_numbers(self):
        for index, nb in enumerate(self.notebooks, 1):
            nb.number_in_section = index


@define
class Course:
    spec: CourseSpec
    course_root: Path
    sections: list[Section] = Factory(list)
    _topic_map: dict[str, Path] = Factory(dict)

    @classmethod
    def from_spec(cls, spec: CourseSpec, course_root: Path) -> "Course":
        course = cls(spec, course_root)
        course.build_sections()
        return course

    def build_sections(self):
        self.build_topic_map()
        for section_spec in self.spec.sections:
            section = Section(name=section_spec.name)
            for topic_spec in section_spec.topics:
                topic_path = self._topic_map.get(topic_spec.id)
                if not topic_path:
                    logging.error(f"Topic not found: {topic_spec.id}")
                    continue
                topic = Topic(
                    id=topic_spec.id,
                    section=section,
                    path=topic_path,
                )
                section.topics.append(topic)
            section.add_notebook_numbers()
            self.sections.append(section)

    def build_topic_map(self, rebuild: bool = False):
        logging.debug(f"Building topic map for {self.course_root}")
        if len(self._topic_map) > 0 and not rebuild:
            return
        self._topic_map.clear()
        for module in (self.course_root / "slides").iterdir():
            for topic in module.iterdir():
                topic_id = simplify_ordered_name(topic.name)
                if self._topic_map.get(topic_id):
                    logging.error(f"Duplicate topic id: {topic_id}")
                    continue
                self._topic_map[topic_id] = topic
        logging.debug(f"Built topic map with {len(self._topic_map)} topics")
