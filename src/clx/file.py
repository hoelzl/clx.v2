import asyncio
from pathlib import Path
from typing import Any, TYPE_CHECKING

from attrs import define, frozen


from clx.operation import Concurrently, NoOperation, Operation
from clx.utils.notebook_utils import find_notebook_titles
from clx.utils.path_utils import (
    PLANTUML_EXTENSIONS,
    ext_for,
    is_slides_file,
    output_specs,
)
from clx.utils.text_utils import Text

if TYPE_CHECKING:
    from clx.course import Section, Topic


@frozen
class DeleteFileOperation(Operation):
    file_to_delete: Path

    async def exec(self, file: "File", *args, **kwargs) -> Any:
        print(f"Deleting {self.file_to_delete.as_posix()}")
        await asyncio.sleep(1)
        file.processing_results.remove(self.file_to_delete)


@define
class File:
    path: Path
    topic: "Topic"
    processing_results: set[Path] = []

    @staticmethod
    def from_path(file: Path, topic: "Topic") -> "File":
        cls: type[File] = _find_file_class(file)
        return cls._from_path(file, topic)

    @classmethod
    def _from_path(cls, file: Path, topic: "Topic") -> "File":
        return cls(path=file, topic=topic)

    @property
    def relative_path(self) -> Path:
        return self.path.relative_to(self.topic.path)

    def output_dir(self, target_dir: Path, lang: str) -> Path:
        return target_dir / self.section.name[lang]

    @property
    def section(self) -> "Section":
        return self.topic.section

    def process_op(self, _target_dir: Path) -> Operation:
        return NoOperation()

    def delete_op(self) -> Operation:
        if self.processing_results:
            return Concurrently(
                DeleteFileOperation(file_to_delete=file)
                for file in self.processing_results
            )
        return NoOperation()


@frozen
class ConvertPlantUmlFile(Operation):
    input_file: "PlantUmlFile"
    output_file: Path

    async def exec(self, file: File, *args, **kwargs) -> Any:
        print(f"Converting PlantUML file {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)
        file.processing_results.add(self.output_file)


@define
class PlantUmlFile(File):
    def process_op(self, _target_dir: Path) -> Operation:
        return ConvertPlantUmlFile(input_file=self, output_file=self.img_path)

    @property
    def img_path(self) -> Path:
        return (self.path.parents[1] / "img").with_suffix(".png")


@frozen
class ConvertDrawIoFile(Operation):
    input_file: "DrawIoFile"
    output_file: Path

    async def exec(self, file: File, *args, **kwargs) -> Any:
        print(f"Converting DrawIO file {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)
        file.processing_results.add(self.output_file)


@define
class DrawIoFile(File):
    def process_op(self, _target_dir: Path) -> Operation:
        return ConvertDrawIoFile(input_file=self, output_file=self.img_path)

    @property
    def img_path(self) -> Path:
        return (self.path.parents[1] / "img").with_suffix(".png")


@frozen
class CopyFileOperation(Operation):
    input_file: "DataFile"
    output_file: Path

    async def exec(self, file: File, *args, **kwargs) -> Any:
        print(f"Copying {self.input_file} to {self.output_file}")
        await asyncio.sleep(1)
        file.processing_results.add(self.output_file)


@define
class DataFile(File):
    def process_op(self, target_dir: Path) -> Operation:
        return Concurrently(
            CopyFileOperation(
                input_file=self,
                output_file=self.output_dir(output_dir, lang) / self.relative_path,
            )
            for lang, _, _, output_dir in output_specs(target_dir)
        )


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str

    async def exec(self, file: File, *args, **kwargs) -> Any:
        print(f"Processing notebook {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)
        file.processing_results.add(self.output_file)


@define
class Notebook(File):
    title: Text = Text(de="", en="")
    number_in_section: int = 0

    @classmethod
    def _from_path(cls, file: Path, topic: "Topic") -> "Notebook":
        text = file.read_text()
        title = find_notebook_titles(text, default=file.stem)
        return cls(path=file, topic=topic, title=title)

    def process_op(self, target_dir: Path) -> Operation:
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
            for lang, format_, mode, output_dir in output_specs(target_dir)
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
