import asyncio
import logging
from pathlib import Path
from typing import Any, TYPE_CHECKING

from attrs import define, field, frozen

from clx.operation import Concurrently, NoOperation, Operation
from clx.utils.execution_uils import FIRST_EXECUTION_STAGE, LAST_EXECUTION_STAGE
from clx.utils.notebook_utils import find_notebook_titles
from clx.utils.path_utils import (
    PLANTUML_EXTENSIONS,
    ext_for,
    is_slides_file,
    output_specs,
)
from clx.utils.text_utils import Text

if TYPE_CHECKING:
    from clx.course import Course, Section, Topic

logger = logging.getLogger(__name__)


OP_DURATION = 0.01


@frozen
class DeleteFileOperation(Operation):
    file: "File"
    file_to_delete: Path

    async def exec(self, *args, **kwargs) -> None:
        logger.info(f"Deleting {self.file_to_delete}")
        await asyncio.sleep(OP_DURATION)
        self.file.generated_outputs.remove(self.file_to_delete)


@define
class File:
    course: "Course"
    path: Path
    topic: "Topic"
    generated_outputs: set[Path] = field(factory=set)

    @staticmethod
    def from_path(course: "Course", file: Path, topic: "Topic") -> "File":
        cls: type[File] = _find_file_class(file)
        return cls._from_path(course, file, topic)

    @classmethod
    def _from_path(cls, course: "Course", file: Path, topic: "Topic") -> "File":
        return cls(course=course, path=file, topic=topic)

    @property
    def execution_stage(self) -> int:
        return FIRST_EXECUTION_STAGE

    @property
    def section(self) -> "Section":
        return self.topic.section

    @property
    def relative_path(self) -> Path:
        return self.path.relative_to(self.topic.path)

    def output_dir(self, target_dir: Path, lang: str) -> Path:
        return target_dir / self.section.name[lang]

    # TODO: Maybe find a better naming convention
    # The generated_outputs are the outputs we have actually generated
    # The generated sources are source-files we *can* generate
    @property
    def generated_sources(self) -> frozenset[Path]:
        return frozenset()

    def get_processing_operation(self, _target_dir: Path) -> Operation:
        return NoOperation()

    def delete_op(self) -> Operation:
        if self.generated_outputs:
            return Concurrently(
                DeleteFileOperation(file=self, file_to_delete=file)
                for file in self.generated_outputs
            )
        return NoOperation()


@frozen
class ConvertPlantUmlFile(Operation):
    input_file: "PlantUmlFile"
    output_file: Path

    async def exec(self, *args, **kwargs) -> None:
        logger.info(
            f"Converting PlantUML file {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await asyncio.sleep(OP_DURATION)
        self.input_file.generated_outputs.add(self.output_file)


@define
class PlantUmlFile(File):
    def get_processing_operation(self, _target_dir: Path) -> Operation:
        return ConvertPlantUmlFile(input_file=self, output_file=self.img_path)

    @property
    def img_path(self) -> Path:
        return (self.path.parents[1] / "img" / self.path.stem).with_suffix(".png")

    @property
    def generated_sources(self) -> frozenset[Path]:
        return frozenset({self.img_path})


@frozen
class ConvertDrawIoFile(Operation):
    input_file: "DrawIoFile"
    output_file: Path

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Converting DrawIO file {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await asyncio.sleep(OP_DURATION)
        self.input_file.generated_outputs.add(self.output_file)


@define
class DrawIoFile(File):
    def get_processing_operation(self, _target_dir: Path) -> Operation:
        return ConvertDrawIoFile(input_file=self, output_file=self.img_path)

    @property
    def img_path(self) -> Path:
        return (self.path.parents[1] / "img" / self.path.stem).with_suffix(".png")

    @property
    def generated_sources(self) -> frozenset[Path]:
        return frozenset({self.img_path})


@frozen
class CopyFileOperation(Operation):
    input_file: "DataFile"
    output_file: Path

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(f"Copying {self.input_file.relative_path} to {self.output_file}")
        await asyncio.sleep(OP_DURATION)
        self.input_file.generated_outputs.add(self.output_file)


@define
class DataFile(File):

    @property
    def execution_stage(self) -> int:
        return LAST_EXECUTION_STAGE

    def get_processing_operation(self, target_dir: Path) -> Operation:
        return Concurrently(
            CopyFileOperation(
                input_file=self,
                output_file=self.output_dir(output_dir, lang) / self.relative_path,
            )
            for lang, _, _, output_dir in output_specs(self.course, target_dir)
        )


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Processing notebook {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await asyncio.sleep(OP_DURATION)
        self.input_file.generated_outputs.add(self.output_file)


@define
class Notebook(File):
    title: Text = Text(de="", en="")
    number_in_section: int = 0

    @classmethod
    def _from_path(cls, course: "Course", file: Path, topic: "Topic") -> "Notebook":
        text = file.read_text()
        title = find_notebook_titles(text, default=file.stem)
        return cls(course=course, path=file, topic=topic, title=title)

    def get_processing_operation(self, target_dir: Path) -> Operation:
        return Concurrently(
            ProcessNotebookOperation(
                input_file=self,
                output_file=(
                    self.output_dir(output_dir, lang)
                    / self.file_name(lang, ext_for(format_))
                ),
                lang=lang,
                format=format_,
                mode=mode,
            )
            for lang, format_, mode, output_dir in output_specs(self.course, target_dir)
        )

    def file_name(self, lang: str, ext: str) -> str:
        return f"{self.number_in_section:02} {self.title[lang]}{ext}"


def _find_file_class(file: Path) -> type[File]:
    if file.suffix in PLANTUML_EXTENSIONS:
        return PlantUmlFile
    if file.suffix == ".drawio":
        return DrawIoFile
    if is_slides_file(file):
        return Notebook
    return DataFile