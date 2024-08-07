import asyncio
import json
import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import Any

import nats
from nats.aio.msg import Msg

from .notebook_processor import NotebookProcessor
from .output_spec import create_output_spec

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "NOTEBOOK_PROCESSOR")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_CELL_PROCESSING = os.environ.get("LOG_CELL_PROCESSING", "False") == "True"

# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - notebook-processor - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()


@dataclass
class NotebookPayload:
    notebook_text: str
    prog_lang: str
    language: str
    notebook_format: str
    output_type: str


@dataclass
class ProcessingInstruction:
    command: str


async def connect_client_with_retry(nats_url: str, num_retries: int = 5):
    for i in range(num_retries):
        try:
            logger.debug(f"Trying to connect to NATS at {nats_url}")
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            await asyncio.sleep(2**i)
    raise OSError("Could not connect to NATS")


async def process_payload(payload: NotebookPayload):
    logger.debug(f"Processing notebook:")

    output_spec = create_output_spec(
        output_type=payload.output_type,
        prog_lang=payload.prog_lang,
        lang=payload.language,
        notebook_format=payload.notebook_format,
    )
    processor = NotebookProcessor(output_spec)
    result = await processor.process_notebook(payload.notebook_text)
    logger.debug(f"Processed notebook: {result[:100]}")
    return result


def try_to_process_notebook_payload(data):
    try:
        payload = NotebookPayload(**data)
        return process_payload(payload)
    except Exception as e:
        logger.error(f"Error processing notebook: {str(e)}")
        return e


class Result(Enum):
    EXIT = 0
    CONTINUE = 1


async def process_message(message: Msg, client: nats.NATS) -> Result:
    data = json.loads(message.data)
    logger.debug(f"Received JSON data: {data}")
    if isinstance(data, dict) and (command := data.get("command")):
        match command:
            case "exit":
                logger.debug("Received exit command")
                response = json.dumps({"info": "Server shutting down"})
                await message.respond(response.encode("utf-8"))
                await client.drain()
                return Result.EXIT
            case _:
                logger.warning("Received unknown command")
                response = json.dumps({"error": "Unknown command"})
                await message.respond(response.encode("utf-8"))
    else:
        try:
            logger.debug("Trying to process notebook payload")
            result = await try_to_process_notebook_payload(data)
            logger.debug(f"Result: {result[:100]}")
            response = json.dumps({"result": str(result)})
            await message.respond(response.encode("utf-8"))
        except Exception as e:
            response = json.dumps({"error": str(e)})
            await message.respond(response.encode("utf-8"))
    return Result.CONTINUE


async def main():
    client = await connect_client_with_retry(NATS_URL)
    subscriber = await client.subscribe("nb.process")
    try:
        async for message in subscriber.messages:
            try:
                result = await process_message(message, client)
                logger.debug(f"Process message returned: {result}")
                if result == Result.EXIT:
                    break
            except json.decoder.JSONDecodeError as e:
                logger.error("JSON decode error: {e}")
            except Exception as e:
                logger.error(f"Error: {e}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())