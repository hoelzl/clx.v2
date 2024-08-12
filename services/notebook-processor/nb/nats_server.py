import asyncio
import json
import logging
import os

import nats
from nats.aio.msg import Msg

from .payload import NotebookPayload
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


async def connect_client_with_retry(nats_url: str, num_retries: int = 5):
    for i in range(num_retries):
        try:
            logger.debug(f"Trying to connect to NATS at {nats_url}")
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except Exception as e:
            logger.exception("Error connecting to NATS: %s", e)
            await asyncio.sleep(2**i)
    raise OSError("Could not connect to NATS")


async def process_payload(payload: NotebookPayload):
    logger.debug(f"Processing notebook")

    output_spec = create_output_spec(
        output_type=payload.output_type,
        prog_lang=payload.prog_lang,
        lang=payload.language,
        notebook_format=payload.notebook_format,
    )
    logger.debug("Output spec created")
    processor = NotebookProcessor(output_spec)
    result = await processor.process_notebook(payload)
    logger.debug(f"Processed notebook: {result[:100]}")
    return result


def try_to_process_notebook_payload(data):
    try:
        payload = NotebookPayload(**data)
        return process_payload(payload)
    except Exception as e:
        logger.exception("Error processing notebook: %s", e)
        raise


async def process_message(message: Msg) -> None:
    try:
        data = json.loads(message.data)
        logger.debug(f"Received JSON data: {data}"[:100])
        result = await try_to_process_notebook_payload(data)
        logger.debug(f"Result: {result[:100]}")
        response = json.dumps({"result": str(result)})
        await message.respond(response.encode("utf-8"))
    except json.decoder.JSONDecodeError as e:
        logger.exception("JSON decode error: %s", e)
    except Exception as e:
        response = json.dumps({"error": str(e)})
        await message.respond(response.encode("utf-8"))


async def main():
    client = await connect_client_with_retry(NATS_URL)
    subscriber = await client.subscribe("nb.process", queue=QUEUE_GROUP)
    try:
        async for message in subscriber.messages:
            try:
                await process_message(message)
            except Exception as e:
                logger.exception("Error while processing message: %s", e)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
