import asyncio
from pathlib import Path
from typing import Any

from attrs import Factory, define, frozen

from clx.operation import Concurrently, NoOperation, Operation
from clx.utils.notebook_utils import find_images, find_imports, find_notebook_titles
from clx.utils.path_utils import Format, Lang, Mode, is_slides_file, output_specs
from clx.utils.text_utils import Text


@define
class File:
    path: Path

    @staticmethod
    def from_path(file: Path) -> "File":
        cls = _find_file_class(file)
        return cls._from_path(file)

    @classmethod
    def _from_path(cls, file: Path) -> "File":
        return cls(path=file)

    def operation(self, _target_dir: Path, _lang, _mode, _format) -> Operation:
        return NoOperation()


@frozen
class ConvertPlantUmlFile(Operation):
    input_file: "PlantUmlFile"
    output_file: Path

    async def exec(self) -> Any:
        print(f"Converting PlantUML file {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)


@define
class PlantUmlFile(File):
    def operation(self, _target_dir: Path, _lang, _mode, _format) -> Operation:
        return ConvertPlantUmlFile(input_file=self, output_file=self._img_path)

    @property
    def _img_path(self) -> Path:
        return (self.path.parents[1] / "img").with_suffix(".png")


@frozen
class ConvertDrawIoFile(Operation):
    input_file: "DrawIoFile"
    output_file: Path

    async def exec(self) -> Any:
        print(f"Converting DrawIO file {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)


@define
class DrawIoFile(File):
    def operation(self, _target_dir: Path, _lang, _mode, _format) -> Operation:
        return ConvertDrawIoFile(input_file=self, output_file=self._img_path)

    @property
    def _img_path(self) -> Path:
        return (self.path.parents[1] / "img").with_suffix(".png")


@frozen
class CopyOperation(Operation):
    input_file: "DataFile"
    output_file: Path

    async def exec(self) -> Any:
        print(f"Copying {self.input_file} to {self.output_file}")
        await asyncio.sleep(1)


@define
class DataFile(File):
    def operation(self, target_dir: Path, _lang, _mode, _format) -> Operation:
        return CopyOperation(input_file=self, output_file=target_dir / self.path.name)


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: Lang
    format: Format
    mode: Mode

    async def exec(self) -> Any:
        print(f"Processing notebook {self.input_file.path} to {self.output_file}")
        await asyncio.sleep(1)


@define
class Notebook(File):
    title: Text = Text(de="", en="")
    number_in_section: int = 0

    @classmethod
    def _from_path(cls, file: Path) -> "Notebook":
        text = file.read_text()
        title = find_notebook_titles(text, default=file.stem)
        return cls(path=file, title=title)

    def operation(self, target_dir: Path, lang, mode, format_) -> Operation:
        return Concurrently(
            ProcessNotebookOperation(
                input_file=self,
                output_file=target_dir / self.path.name,
                lang=lang,
                format=format_,
                mode=mode,
            )
            for lang, format_, mode, output_dir in output_specs(target_dir)
        )


def _find_file_class(file: Path) -> type[File]:
    if file.suffix == ".puml":
        return PlantUmlFile
    if file.suffix == ".drawio":
        return DrawIoFile
    if is_slides_file(file):
        return Notebook
    return DataFile

