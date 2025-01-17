import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from attrs import Factory, define

from clx.course_file import CourseFile, Notebook
from clx.course_spec import CourseSpec
from clx.dict_group import DictGroup
from clx.section import Section
from clx.topic import Topic
from clx.utils.div_uils import File, execution_stages
from clx.utils.path_utils import (is_ignored_dir_for_course,
    is_in_dir, simplify_ordered_name,
)
from clx.utils.text_utils import Text

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def chunks(lst: list, n: int) -> list[list]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


@define
class Course:
    spec: CourseSpec
    course_root: Path
    output_root: Path
    sections: list[Section] = Factory(list)
    dict_groups: list[DictGroup] = Factory(list)
    _topic_path_map: dict[str, Path] = Factory(dict)

    @classmethod
    def from_spec(
        cls, spec: CourseSpec, course_root: Path, output_root: Path | None
    ) -> "Course":
        if output_root is None:
            output_root = course_root / "output"
        logger.debug(
            f"Creating course from spec {spec}: " f"{course_root} -> {output_root}"
        )
        course = cls(spec, course_root, output_root)
        course._build_sections()
        course._build_dict_groups()
        course._add_generated_sources()
        return course

    @property
    def name(self) -> Text:
        return self.spec.name

    @property
    def topics(self) -> list[Topic]:
        return [topic for section in self.sections for topic in section.topics]

    @property
    def files(self) -> list[CourseFile]:
        return [file for section in self.sections for file in section.files]

    def find_file(self, path) -> File | None:
        abspath = path.resolve()
        for dir_group in self.dict_groups:
            for source_dir in dir_group.source_dirs:
                if is_in_dir(abspath, source_dir):
                    return File(path=abspath)
        return self.find_course_file(abspath)

    def find_course_file(self, path: Path) -> CourseFile | None:
        abspath = path.resolve()
        for file in self.files:
            if file.path.resolve() == abspath:
                return file
        return None

    def add_file(self, path: Path, warn_if_no_topic: bool = True) -> Topic | None:
        for topic in self.topics:
            if topic.matches_path(path, False):
                topic.add_file(path)
                return topic
        if warn_if_no_topic:
            logger.warning(f"File not in course structure: {path}")
        else:
            logger.debug(f"File not in course structure: {path}")
        return None

    @property
    def notebooks(self) -> list[Notebook]:
        return [file for file in self.files if isinstance(file, Notebook)]

    async def on_file_moved(self, src_path: Path, dest_path: Path):
        logger.debug(f"On file moved: {src_path} -> {dest_path}")
        await self.on_file_deleted(src_path)
        await self.on_file_created(dest_path)

    async def on_file_deleted(self, file_to_delete: Path):
        logger.info(f"On file deleted: {file_to_delete}")
        file = self.find_course_file(file_to_delete)
        if not file:
            logger.debug(f"File not / no longer in course: {file_to_delete}")
            return
        await file.delete()

    async def on_file_created(self, path: Path):
        logger.debug(f"On file created: {path}")
        topic = self.add_file(path, warn_if_no_topic=False)
        if topic is not None:
            await self.process_file(path)
        else:
            logger.debug(f"File not in course: {path}")

    async def on_file_modified(self, path: Path):
        logger.info(f"On file modified: {path}")
        if self.find_course_file(path):
            await self.process_file(path)

    async def process_file(self, path: Path):
        logging.info(f"Processing changed file {path}")
        file = self.find_course_file(path)
        if not file:
            logger.warning(f"Cannot process file: not in course: {path}")
            return
        op = await file.get_processing_operation(self.output_root)
        await op.exec()
        logger.debug(f"Processed file {path}")

    async def process_all(self):
        logger.info(f"Processing all files for {self.course_root}")
        for section in self.sections:
            logger.debug(f"Processing section {section.name}")
            for stage in execution_stages():
                logger.debug(f"Processing stage {stage} for section {section.name}")
                operations = []
                for file in section.files:
                    if file.execution_stage == stage:
                        logger.debug(f"Processing file {file.path}")
                        operations.append(
                            await file.get_processing_operation(self.output_root)
                        )
                op_chunks = chunks(operations, 10)
                for op_chunk in op_chunks:
                    await asyncio.gather(
                        *[op.exec() for op in op_chunk], return_exceptions=True
                    )
                    await asyncio.sleep(0.5)
                logger.debug(f"Processed {len(operations)} files for stage {stage}")

        operations = []
        for dict_group in self.dict_groups:
            logger.debug(f"Processing dict group {dict_group.name}")
            operations.append(await dict_group.get_processing_operation())
        await asyncio.gather(*[op.exec() for op in operations],
            return_exceptions=True)

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
            topic_path = self._topic_path_map.get(topic_spec.id)
            if not topic_path:
                logger.error(f"Topic not found: {topic_spec.id}")
                continue
            topic = Topic.from_id(id=topic_spec.id, section=section, path=topic_path)
            topic.build_file_map()
            section.topics.append(topic)

    def _build_topic_map(self, rebuild: bool = False):
        logger.debug(f"Building topic map for {self.course_root}")
        if len(self._topic_path_map) > 0 and not rebuild:
            return
        self._topic_path_map.clear()
        for module in (self.course_root / "slides").iterdir():
            if is_ignored_dir_for_course(module):
                logger.debug(f"Skipping ignored dir while building topic map: {module}")
                continue
            if not module.is_dir():
                logger.debug(
                    "Skipping non-directory module while building topic map: "
                    f"{module}"
                )
                continue
            for topic_path in module.iterdir():
                topic_id = simplify_ordered_name(topic_path.name)
                if not topic_id:
                    logger.debug(f"Skipping topic with no id: {topic_path}")
                    continue
                if existing_topic_path := self._topic_path_map.get(topic_id):
                    logger.warning(
                        f"Duplicate topic id: {topic_id}: "
                        f"{topic_path} and {existing_topic_path}"
                    )
                    continue
                self._topic_path_map[topic_id] = topic_path
        logger.debug(f"Built topic map with {len(self._topic_path_map)} topics")

    def _build_dict_groups(self):
        for dictionary_spec in self.spec.dictionaries:
            self.dict_groups.append(DictGroup.from_spec(dictionary_spec, self))

    def _add_generated_sources(self):
        logger.debug("Adding generated sources.")
        for topic in self.topics:
            for file in topic.files:
                for new_file in file.generated_sources:
                    topic.add_file(new_file)
                    logger.debug(f"Added generated source: {new_file}")
