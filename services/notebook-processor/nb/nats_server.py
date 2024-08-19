import asyncio
import json
import logging
import os

import nats
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig

from .notebook_processor import NotebookProcessor
from .output_spec import create_output_spec
from .payload import NotebookPayload

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("NOTEBOOK_PROCESSOR_QUEUE_GROUP", "NOTEBOOK_PROCESSOR")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
LOG_CELL_PROCESSING = os.environ.get("LOG_CELL_PROCESSING", "False") == "True"

NB_PROCESS_SUBJECT = "notebook.process"
NB_PROCESS_STREAM = "NOTEBOOK_PROCESS_STREAM"
NB_RESULT_STREAM = "NOTEBOOK_RESULT_STREAM"
NB_RESULT_SUBJECT = "notebook.result"


# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - notebook-processor - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global flag to signal shutdown
shutdown_flag = asyncio.Event()



async def extract_payload(msg):
    try:
        data = json.loads(msg.data)
        logger.debug(f"Received JSON data: {data}")
        payload = NotebookPayload(**data)
        return payload
    except json.JSONDecodeError as e:
        logger.exception("JSON decode error: %s", e)
        raise

async def process_notebook_file(payload: NotebookPayload) -> str:
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
    logger.debug(f"Processed notebook: {processed_notebook[:60]}")
    return processed_notebook


class NotebookConverter:
    def __init__(self):
        self.nats_client: nats.NATS | None = None
        self.jetstream: JetStreamContext | None = None
        self.subscription: JetStreamContext.PushSubscription | None = None

    async def run(self):
        await self.connect_nats()
        await self.subscribe_to_events()
        try:
            await self.fetch_and_process_messages()
        finally:
            if self.nats_client:
                await self.nats_client.close()

    async def connect_nats(self):
        try:
            self.nats_client = await nats.connect(NATS_URL)
            self.jetstream = self.nats_client.jetstream()
            logger.info(f"Connected to NATS at {NATS_URL}")
        except Exception as e:
            logger.exception("Error connecting to NATS: %s", e)
            raise

    async def subscribe_to_events(self):
        subject = NB_PROCESS_SUBJECT
        stream = NB_PROCESS_STREAM
        logger.debug(f"Subscribing to subject: '{subject}' on stream '{stream}'")
        config = ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            max_deliver=1,
        )
        self.subscription = await self.jetstream.subscribe(
            subject=subject, queue=QUEUE_GROUP, stream=stream, config=config
        )
        logger.info(f"Subscribed to subject: '{subject}' on stream '{stream}'")

    async def fetch_and_process_messages(self):
        while True:
            try:
                await self.fetch_and_process_one_message()
            except TimeoutError:
                continue
            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down...")
            except Exception as e:
                logger.exception(f"Error while handling event: {e}", exc_info=e)
                # Sleep to limit resources when we have an error inside the program...
                await asyncio.sleep(1.0)
                continue

    async def fetch_and_process_one_message(self):
        msg = await self.subscription.next_msg()
        await msg.ack()
        logger.debug(f"Processing message {msg.data[:40]}")
        await self.process_message(msg)

    async def process_message(self, msg):
        payload = await extract_payload(msg)
        try:
            result = await process_notebook_file(payload)
            logger.debug(f"Result: {result[:60]}")
            response = json.dumps({"result": result})
            await self.publish_response(payload.reply_subject, response)
        except Exception as e:
            logger.exception(f"Error while processing notebook: {e}", exc_info=e)
            await self.jetstream.publish(
                subject=payload.reply_subject,
                stream=NB_RESULT_STREAM,
                payload=json.dumps({"error": str(e)}).encode("utf-8"),
            )

    async def publish_response(self, reply_subject, response):
        result_stream = NB_RESULT_STREAM
        logger.debug(
            f"Sending reply for subject '{reply_subject}' on "
            f"stream '{result_stream}'."
        )
        await self.jetstream.publish(
            subject=reply_subject,
            stream=result_stream,
            payload=response.encode("utf-8"),
        )



if __name__ == "__main__":
    converter = NotebookConverter()
    asyncio.run(converter.run())
