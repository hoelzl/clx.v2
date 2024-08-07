import asyncio
import json
import logging
import os
import shutil
from base64 import b64decode
from pathlib import Path
from typing import Any, TYPE_CHECKING

import aiofiles
import nats
from attr import frozen

from clx.operation import Operation

if TYPE_CHECKING:
    from clx.file import DataFile, DrawIoFile, File, Notebook, PlantUmlFile


logger = logging.getLogger(__name__)

OP_DURATION = 0.01
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def connect_client_with_retry(nats_url: str, num_retries: int = 5):
    for i in range(num_retries):
        try:
            logger.debug(f"Trying to connect to NATS at {nats_url}")
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            await asyncio.sleep(0.25 * 2**i)
    raise OSError("Could not connect to NATS")


async def process_image_request(file, service: str, nats_topic: str, timeout=120):
    try:
        nc = await connect_client_with_retry(NATS_URL)
        payload = {
            "data": file.input_file.path.read_text(),
            "output_format": "png",
        }
        reply = await nc.request(
            nats_topic, json.dumps(payload).encode(), timeout=timeout
        )
        logger.debug(f"{service}: Received reply: {reply.data[:40]}")
        result = json.loads(reply.data.decode())
        if isinstance(result, dict):
            if png_base64 := result.get("result").encode():
                logger.debug(
                    f"{service}: PNG data: len = {len(png_base64)}, {png_base64[:20]}"
                )
                png = b64decode(png_base64)
                logger.debug(f"{service}: Writing PNG data to {file.output_file}")
                file.output_file.write_bytes(png)
    except Exception as e:
        logger.error(f"{service}: Error {e}")


@frozen
class DeleteFileOperation(Operation):
    file: "File"
    file_to_delete: Path

    async def exec(self, *args, **kwargs) -> None:
        logger.info(f"Deleting {self.file_to_delete}")
        self.file_to_delete.unlink()
        self.file.generated_outputs.remove(self.file_to_delete)


@frozen
class ConvertPlantUmlFile(Operation):
    input_file: "PlantUmlFile"
    output_file: Path

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
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str
    prog_lang: str = "python"

    async def exec(self, *args, **kwargs) -> Any:
        logger.info(
            f"Processing notebook {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await self.process_request()
        self.input_file.generated_outputs.add(self.output_file)

    async def process_request(self, timeout=120):
        try:
            nc = await connect_client_with_retry(NATS_URL)
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
