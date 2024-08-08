import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any, TYPE_CHECKING

import nats
from attr import frozen

from clx.operation import Operation
from clx.utils.nats_utils import process_image_request

if TYPE_CHECKING:
    from clx.course import DictGroup
    from clx.course_file import DataFile, DrawIoFile, CourseFile, Notebook, PlantUmlFile


logger = logging.getLogger(__name__)

OP_DURATION = 0.01


@frozen
class DeleteFileOperation(Operation):
    file: "CourseFile"
    file_to_delete: Path

    async def exec(self, *args, **kwargs) -> None:
        logger.info(f"Deleting {self.file_to_delete}")
        self.file_to_delete.unlink()
        self.file.generated_outputs.remove(self.file_to_delete)


@frozen
class ConvertPlantUmlFile(Operation):
    input_file: "PlantUmlFile"
    output_file: Path
    nats_connection: nats.NATS

    async def exec(self, *args, **kwargs) -> None:
        logger.info(
            f"Converting PlantUML file {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await self.process_request()
        self.input_file.generated_outputs.add(self.output_file)

    async def process_request(self, timeout=120):
        await process_image_request(
            self, "PlantUML", "plantuml.process", timeout=timeout
        )


@frozen
class ConvertDrawIoFile(Operation):
    input_file: "DrawIoFile"
    output_file: Path
    nats_connection: nats.NATS

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Converting DrawIO file {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await self.process_request()
        self.input_file.generated_outputs.add(self.output_file)

    async def process_request(self, timeout=120):
        await process_image_request(self, "DrawIO", "drawio.process", timeout=timeout)


@frozen
class CopyFileOperation(Operation):
    input_file: "DataFile"
    output_file: Path

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(f"Copying {self.input_file.relative_path} to {self.output_file}")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.input_file.path, self.output_file)
        self.input_file.generated_outputs.add(self.output_file)


@frozen
class CopyDictGroupOperation(Operation):
    dict_group: "DictGroup"
    lang: str

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(f"Copying {self.dict_group.output_path(is_speaker, self.lang)}")
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self.dict_group.copy_to_output(is_speaker,
                                                                        self.lang))


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str
    prog_lang: str
    nats_connection: nats.NATS

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Processing notebook '{self.input_file.relative_path}' "
            f"to '{self.output_file}'"
        )
        await self.process_request()
        self.input_file.generated_outputs.add(self.output_file)

    async def process_request(self, timeout=120):
        try:
            nc = self.nats_connection
            logger.debug(f"Notebook: Preparing payload {self.input_file.path}")
            payload = {
                "prog_lang": self.prog_lang,
                "language": self.lang,
                "notebook_format": self.format,
                "output_type": self.mode,
                "notebook_text": self.input_file.path.read_text(),
            }
            logger.debug(f"Notebook: sending request: {payload}")
            reply = await nc.request(
                "nb.process", json.dumps(payload).encode(), timeout=timeout
            )
            logger.debug(f"Notebook: Received reply")
            result = json.loads(reply.data.decode())
            logger.debug(f"Notebook: Decoded Reply")
            if isinstance(result, dict):
                if notebook := result.get("result"):
                    logger.debug(f"Notebook: Writing notebook to {self.output_file}")
                    self.output_file.parent.mkdir(parents=True, exist_ok=True)
                    self.output_file.write_text(notebook)
                else:
                    logger.error(f"Notebook: no result key {result}")
            else:
                logger.error(f"Notebook: reply not a dict {result}")
        except Exception as e:
            logger.error(f"Notebook: Error {e}")
