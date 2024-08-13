import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Any, TYPE_CHECKING

import nats
from attrs import define, frozen

from clx.operation import Operation
from clx.utils.nats_utils import process_image_request
from clx.utils.path_utils import is_image_file, is_image_source_file
from clx.utils.text_utils import sanitize_stream_name, unescape

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

    async def process_request(self, timeout=240):
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
        if not self.output_file.parent.exists():
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(self.input_file.path, self.output_file)
        self.input_file.generated_outputs.add(self.output_file)


@frozen
class CopyDictGroupOperation(Operation):
    dict_group: "DictGroup"
    lang: str

    async def exec(self, *args, **kwargs) -> Any:
        logger.debug(f"Copying dict group {self.dict_group.name} for " f"{self.lang}")
        for is_speaker in [True, False]:
            logger.info(f"Copying {self.dict_group.output_path(is_speaker, self.lang)}")
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, self.dict_group.copy_to_output(is_speaker, self.lang)
            )


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str
    prog_lang: str
    nats_connection: nats.NATS

    @property
    def reply_subject(self) -> str:
        id_ = self.input_file.topic.id
        num = self.input_file.number_in_section
        lang = self.lang
        format_ = self.format
        mode = self.mode
        reply_topic = f"{id_}_{num}_{lang}_{format_}_{mode}"
        return sanitize_stream_name(f"nb.completed.{reply_topic}")

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Processing notebook '{self.input_file.relative_path}' "
            f"to '{self.output_file}'"
        )
        await self.process_request()
        self.input_file.generated_outputs.add(self.output_file)

    async def process_request(self):
        try:
            nc = self.nats_connection
            logger.debug(f"Processing {self.input_file.relative_path} ")
            sub = await self.nats_connection.subscribe(self.reply_subject)
            payload = await self.build_payload()
            logger.debug(f"Notebook: sending request: {payload}")
            await nc.publish("nb.process", json.dumps(payload).encode())
            logger.debug(
                f"Notebook: Waiting for processed notebook {self.reply_subject}"
            )
            await self.handle_message(sub)
            logger.debug(f"Notebook: done with {self.reply_subject} ")
            await sub.drain()
        except Exception as e:
            logger.exception("Notebook: Error while processing request: %s", e)

    async def handle_message(self, sub):
        while True:
            try:
                msg = await sub.next_msg(timeout=1)
                self.write_notebook_to_file(msg)
                break
            except asyncio.TimeoutError:
                continue

    def write_notebook_to_file(self, msg):
        data = json.loads(msg.data.decode())
        logger.debug(f"Notebook: Decoded message {str(data)[:50]}")
        if isinstance(data, dict):
            if notebook := data.get("result"):
                logger.debug(f"Notebook: Writing notebook to {self.output_file}")
                if not self.output_file.parent.exists():
                    self.output_file.parent.mkdir(parents=True, exist_ok=True)
                self.output_file.write_text(notebook)
            elif error := data.get("error"):
                logger.error(f"Notebook: Error: {error}")
            else:
                logger.error(f"Notebook: no key 'result' in {unescape(data)}")
        else:
            logger.error(f"Notebook: reply not a dict {unescape(data)}")

    async def build_payload(self):
        notebook_path = self.input_file.relative_path.name
        return {
            "notebook_text": self.input_file.path.read_text(),
            "notebook_path": notebook_path,
            "reply_subject": self.reply_subject,
            "prog_lang": self.prog_lang,
            "language": self.lang,
            "notebook_format": self.format,
            "output_type": self.mode,
            "other_files": {
                str(file.relative_path): file.path.read_text()
                for file in self.input_file.topic.files
                if file != self.input_file
                and not is_image_file(file.path)
                and not is_image_source_file(file.path)
            },
        }
