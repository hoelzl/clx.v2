import asyncio
import json
import logging
import os
from dataclasses import dataclass

import nats
from nats.aio.msg import Msg

from .payload import NotebookPayload
from .notebook_processor import NotebookProcessor
from .output_spec import create_output_spec

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("NOTEBOOK_PROCESSOR_QUEUE_GROUP", "NOTEBOOK_PROCESSOR")

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
class ProcessingResult:
    processed_notebook: str

    @property
    def result_payload(self) -> bytes:
        return json.dumps({"result": self.processed_notebook}).encode("utf-8")


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


async def process_payload(payload: NotebookPayload) -> ProcessingResult:
    logger.debug(f"Processing notebook payload for '{payload.reply_subject}'")
    output_spec = create_output_spec(
        output_type=payload.output_type,
        prog_lang=payload.prog_lang,
        lang=payload.language,
        notebook_format=payload.notebook_format,
    )
    logger.debug("Output spec created")
    processor = NotebookProcessor(output_spec)
    processed_notebook = await processor.process_notebook(payload)
    logger.debug(f"Processed notebook: {processed_notebook[:100]}")
    return ProcessingResult(processed_notebook)


async def process_message(message: Msg, client: nats.NATS) -> None:
    try:
        payload = handle_incoming_message(message)
        # await asyncio.create_task(process_and_publish(payload, client))
        await process_and_publish(payload, client)
    except Exception as e:
        logger.exception("Error handling incoming message: %s", e)



def handle_incoming_message(message):
    try:
        data = json.loads(message.data)
        payload = NotebookPayload(**data)
        logger.debug(f"Received JSON data: {data}"[:60])
        return payload
    except Exception as e2:
        logger.exception("Error handling incoming message: %s", e2)
        raise


async def process_and_publish(payload: NotebookPayload, client: nats.NATS):
    try:
        result = await process_payload(payload)
        logger.debug(f"Result: {result.processed_notebook[:60]}")
        await client.publish(payload.reply_subject, result.result_payload)
    except Exception as e:
        logger.exception("Error processing notebook: %s", e)
        response = json.dumps({"error": str(e)})
        await client.publish(payload.reply_subject, response.encode("utf-8"))


async def main():
    client = await connect_client_with_retry(NATS_URL)
    subscriber = await client.subscribe("nb.process", queue=QUEUE_GROUP)
    await client.flush()
    try:
        async for message in subscriber.messages:
            await process_message(message, client)
            # await asyncio.sleep(0.1)  # Short delay before processing the next message
    finally:
        await client.close()



if __name__ == "__main__":
    asyncio.run(main())
