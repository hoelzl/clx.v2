import asyncio
import json
import logging
import os
import re
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import nats
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig

# Configuration
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("PLANTUML_CONVERTER_QUEUE_GROUP", "PLANTUML_CONVERTER")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()

PLANTUML_PROCESS_SUBJECT = "plantuml.process"
PLANTUML_PROCESS_STREAM = "PLANTUML_PROCESS_STREAM"
IMG_RESULT_STREAM = "IMG_RESULT_STREAM"

PLANTUML_NAME_REGEX = re.compile(r'@startuml[ \t]+(?:"([^"]+)"|(\S+))')

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - plantuml-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def get_plantuml_output_name(content, default="plantuml"):
    match = PLANTUML_NAME_REGEX.search(content)
    if match:
        name = match.group(1) or match.group(2)
        # Output name most likely commented out
        # This is not entirely accurate, but good enough for our purposes
        if "'" in name:
            return default
        return name
    return default


@dataclass
class PlantUmlPayload:
    data: str
    reply_subject: str
    output_format: str = "png"


async def extract_payload(msg):
    try:
        data = json.loads(msg.data)
        logger.debug(f"Received JSON data: {data}")
        payload = PlantUmlPayload(**data)
        return payload
    except json.JSONDecodeError as e:
        logger.exception("JSON decode error: %s", e)
        raise


class PlantUmlConverter:
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
        subject = PLANTUML_PROCESS_SUBJECT
        stream = PLANTUML_PROCESS_STREAM
        logger.debug(f"Subscribing to subject: {subject} on stream {stream}")
        config = ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            max_deliver=1,
        )
        self.subscription = await self.jetstream.subscribe(
            subject=subject, queue=QUEUE_GROUP, stream=stream, config=config
        )
        logger.info(f"Subscribed to subject: {subject} on stream {stream}")

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
        msg = await self.subscription.next_msg(timeout=None)
        await msg.ack()
        logger.debug(f"Processing message {msg.data[:40]}")
        await self.process_message(msg)

    async def process_message(self, msg):
        payload = await extract_payload(msg)
        try:
            result = await self.process_plantuml_file(payload)
            logger.debug(f"Raw result: {len(result)} bytes")
            encoded_result = b64encode(result)
            logger.debug(f"Result: {len(result)} bytes: {encoded_result[:20]}")
            response = json.dumps({"result": encoded_result.decode("utf-8")})
            await self.publish_response(payload.reply_subject, response)
        except Exception as e:
            logger.exception(f"Error while processing PlantUML file: {e}", exc_info=e)
            await self.jetstream.publish(
                subject=payload.reply_subject,
                stream=IMG_RESULT_STREAM,
                payload=json.dumps({"error": str(e)}).encode("utf-8"),
            )

    async def publish_response(self, reply_subject, response):
        result_stream = IMG_RESULT_STREAM
        logger.debug(
            f"Sending reply for subject '{reply_subject}' on "
            f"stream '{result_stream}'."
        )
        await self.jetstream.publish(
            subject=reply_subject,
            stream=result_stream,
            payload=response.encode("utf-8"),
        )

    async def process_plantuml_file(self, data: PlantUmlPayload) -> bytes:
        logger.debug(f"Processing PlantUML file: {data}")
        with TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "plantuml.pu"
            output_name = get_plantuml_output_name(data.data, default="plantuml")
            output_path = (Path(tmp_dir) / output_name).with_suffix(
                f".{data.output_format}"
            )
            logger.debug(f"Input path: {input_path}, output path: {output_path}")
            with open(input_path, "w") as f:
                f.write(data.data)
            await self.convert_plantuml(input_path)
            for file in output_path.parent.iterdir():
                logger.debug(f"Found file: {file}")
            return output_path.read_bytes()

    @staticmethod
    async def convert_plantuml(input_file: Path):
        logger.debug(f"Converting PlantUML file: {input_file}")
        cmd = [
            "java",
            "-jar",
            "/app/plantuml.jar",
            "-tpng",
            "-Sdpi=600",
            "-o",
            str(input_file.parent),
            str(input_file),
        ]

        logger.debug("Creating subprocess...")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        logger.debug("Waiting for conversion to complete...")
        stdout, stderr = await process.communicate()

        if process.returncode == 0:
            logger.info(f"Converted {input_file}")
        else:
            logger.error(f"Error converting {input_file}: {stderr.decode()}")


if __name__ == "__main__":
    converter = PlantUmlConverter()
    asyncio.run(converter.run())
