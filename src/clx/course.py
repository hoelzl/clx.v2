import asyncio
import logging
from pathlib import Path

from attrs import define, frozen, Factory
from clx.course_spec import CourseSpec
from clx.file import File, Notebook
from clx.utils.execution_uils import execution_stages
from clx.utils.path_utils import is_ignored_dir_for_course, simplify_ordered_name
from clx.utils.text_utils import Text

logger = logging.getLogger(__name__)


@frozen
class Topic:
    id: str
    section: "Section"
    path: Path
    _file_map: dict[Path, File] = Factory(dict)

    @property
    def course(self) -> "Course":
        return self.section.course

    @property
    def files(self) -> list[File]:
        return list(self._file_map.values())

    @property
    def notebooks(self) -> list[Notebook]:
        return [file for file in self.files if isinstance(file, Notebook)]

    def file_for_path(self, path: Path) -> File:
        return self._file_map.get(path)

    def add_file(self, path: Path):
        if self.file_for_path(path):
            logger.debug(f"Duplicate path when adding file: {path}")
            return
        if path.is_dir():
            logger.error(f"Trying to add a directory to topic {self.id!r}: {path}")
            return
        self._file_map[path] = File.from_path(self.course, path, self)

    def build_file_map(self):
        logger.debug(f"Building file map for {self.path}")
        for file in sorted(list(self.path.iterdir())):
            if file.is_file():
                self.add_file(file)
            elif file.is_dir() and not is_ignored_dir_for_course(file):
                for sub_file in file.glob("**/*"):
                    self.add_file(sub_file)


@define
class Section:
    name: Text
    course: "Course"
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
    output_root: Path
    sections: list[Section] = Factory(list)
    _topic_map: dict[str, Path] = Factory(dict)

    @classmethod
    def from_spec(
        cls, spec: CourseSpec, course_root: Path, output_root: Path | None
    ) -> "Course":
        if output_root is None:
            output_root = course_root / "output"
        logger.debug(f"Creating course from spec {spec}: "
                     f"{course_root} -> {output_root}")
        course = cls(spec, course_root, output_root)
        course._build_sections()
        course._add_generated_sources()
        return course

    @property
    def name(self) -> Text:
        return self.spec.name

    @property
    def files(self) -> list[File]:
        return [file for section in self.sections for file in section.files]

    @property
    def notebooks(self) -> list[Notebook]:
        return [file for file in self.files if isinstance(file, Notebook)]

    async def process_all(self):
        logger.debug(f"Processing all files for {self.course_root}")
        for stage in execution_stages():
            logger.debug(f"Processing stage {stage}")
            operations = []
            for file in self.files:
                if file.execution_stage == stage:
                    operations.append(file.get_processing_operation(self.output_root))
            await asyncio.gather(
                *[op.exec() for op in operations], return_exceptions=True
            )
            logger.debug(f"Processed {len(operations)} files for stage {stage}")

    def _build_sections(self):
        logger.debug(f"Building sections for {self.course_root}")
        self._build_topic_map()
        for section_spec in self.spec.sections:
            section = Section(name=section_spec.name, course=self)
            self._build_topics(section, section_spec)
            section.add_notebook_numbers()
            self.sections.append(section)

    def _build_topics(self, section, section_spec):
        for topic_spec in section_spec.topics:
            topic_path = self._topic_map.get(topic_spec.id)
            if not topic_path:
                logger.error(f"Topic not found: {topic_spec.id}")
                continue
            topic = Topic(id=topic_spec.id, section=section, path=topic_path)
            topic.build_file_map()
            section.topics.append(topic)

    def _build_topic_map(self, rebuild: bool = False):
        logger.debug(f"Building topic map for {self.course_root}")
        if len(self._topic_map) > 0 and not rebuild:
            return
        self._topic_map.clear()
        for module in (self.course_root / "slides").iterdir():
            for topic in module.iterdir():
                topic_id = simplify_ordered_name(topic.name)
                if self._topic_map.get(topic_id):
                    logger.error(f"Duplicate topic id: {topic_id}")
                    continue
                self._topic_map[topic_id] = topic
        logger.debug(f"Built topic map with {len(self._topic_map)} topics")

    def _add_generated_sources(self):
        logger.debug("Adding generated sources.")
        for section in self.sections:
            for topic in section.topics:
                for file in topic.files:
                    for new_file in file.generated_sources:
                        topic.add_file(new_file)
                        logger.debug(f"Added generated source: {new_file}")
