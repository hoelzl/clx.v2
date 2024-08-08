import asyncio
import logging
from pathlib import Path

import nats
from attrs import define, frozen, Factory
from clx.course_spec import CourseSpec, DictGroupSpec
from clx.file import File, Notebook
from clx.utils.execution_uils import execution_stages
from clx.utils.nats_utils import NATS_URL, connect_client_with_retry
from clx.utils.path_utils import (
    is_ignored_dir_for_course,
    output_path_for,
    simplify_ordered_name,
)
from clx.utils.text_utils import Text, sanitize_file_name

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
        # We can add files that don't exist yet (e.g. generated files), so don't check
        # if the path resolves to a file.
        if not self.matches_path(path, False):
            logger.debug(f"Path not within topic: {path}")
            return
        if self.file_for_path(path):
            logger.debug(f"Duplicate path when adding file: {path}")
            return
        if path.is_dir():
            logger.error(f"Trying to add a directory to topic {self.id!r}: {path}")
            return
        try:
            self._file_map[path] = File.from_path(self.course, path, self)
        except Exception as e:
            logger.error(f"Error adding file {path}: {e}")

    def matches_path(self, path: Path, check_is_file: bool = True) -> bool:
        """Returns True if the path is within the topic directory."""
        if self.path.resolve() in path.resolve().parents:
            if check_is_file:
                return path.is_file()
            return True
        return False

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


@frozen
class DictGroup:
    name: Text
    source_dirs: tuple[Path, ...]
    relative_paths: tuple[Path, ...]
    course: "Course"
    include_top_level_files: bool = True

    @classmethod
    def from_spec(cls, spec: DictGroupSpec, course: "Course") -> "DictGroup":
        source_path = course.course_root / spec.path
        source_dirs = (
            tuple(source_path / subdir for subdir in spec.subdirs)
            if spec.subdirs
            else (source_path,)
        )
        relative_paths = tuple(Path(path) for path in spec.subdirs or [""])
        return cls(
            name=spec.name,
            source_dirs=source_dirs,
            relative_paths=relative_paths,
            course=course,
            include_top_level_files=spec.include_top_level_files,
        )

    @property
    def output_root(self) -> Path:
        return self.course.output_root

    def output_path(self, lang: str) -> Path:
        return (
            output_path_for(self.output_root, lang, self.course.name) / self.name[lang]
        )

    def output_dirs(self, lang: str) -> tuple[Path, ...]:
        return tuple(self.output_path(lang) / dir_ for dir_ in self.relative_paths)

    def copy_to_output(self, lang: str):
        for source_dir, relative_path in zip(self.source_dirs, self.relative_paths):
            output_dir = self.output_path(lang) / relative_path
            output_dir.mkdir(parents=True, exist_ok=True)
            for file in source_dir.glob("**/*"):
                if file.is_file():
                    target = output_dir / file.relative_to(source_dir)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(file.read_bytes())
                    logger.debug(f"Copied {file} to {target}")


@define
class Course:
    spec: CourseSpec
    course_root: Path
    output_root: Path
    sections: list[Section] = Factory(list)
    dict_groups: list[DictGroup] = Factory(list)
    _topic_map: dict[str, Path] = Factory(dict)
    _nats_connection: nats.NATS | None = None

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
    def files(self) -> list[File]:
        return [file for section in self.sections for file in section.files]

    def find_file(self, path):
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

    async def nats_connection(self):
        if not self._nats_connection:
            self._nats_connection = await connect_client_with_retry(NATS_URL)
        return self._nats_connection

    async def on_file_moved(self, src_path: Path, dest_path: Path):
        logger.debug(f"On file moved: {src_path} -> {dest_path}")
        await self.on_file_deleted(src_path)
        await self.on_file_created(dest_path)

    async def on_file_created(self, path: Path):
        logger.debug(f"On file created: {path}")
        topic = self.add_file(path, warn_if_no_topic=False)
        if topic is not None:
            await self.process_file(path)
        else:
            logger.debug(f"File not in course: {path}")

    async def on_file_deleted(self, file_to_delete: Path):
        logger.info(f"On file deleted: {file_to_delete}")
        file = self.find_file(file_to_delete)
        if not file:
            logger.debug(f"File not / no longer in course: {file_to_delete}")
            return
        await file.delete()

    async def on_file_modified(self, path: Path):
        logger.info(f"On file modified: {path}")
        if self.find_file(path):
            await self.process_file(path)

    async def process_file(self, path: Path):
        logging.info(f"Processing changed file {path}")
        file = self.find_file(path)
        if not file:
            logger.error(f"Cannot process file: not in course: {path}")
            return
        op = await file.get_processing_operation(self.output_root)
        await op.exec()
        logger.debug(f"Processed file {path}")

    async def process_all(self):
        logger.info(f"Processing all files for {self.course_root}")
        for stage in execution_stages():
            logger.debug(f"Processing stage {stage}")
            operations = []
            for file in self.files:
                if file.execution_stage == stage:
                    operations.append(
                        await file.get_processing_operation(self.output_root)
                    )
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
