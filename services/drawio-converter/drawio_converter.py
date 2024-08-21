import asyncio
import json
import logging
import os
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import nats
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig

# Configuration
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("DRAWIO_CONVERTER_QUEUE_GROUP", "DRAWIO_CONVERTER")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()

DRAWIO_PROCESS_ROUTING_KEY = "drawio.process"
DRAWIO_PROCESS_STREAM = "DRAWIO_PROCESS_STREAM"
IMG_RESULT_STREAM = "IMG_RESULT_STREAM"

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - drawio-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class DrawioPayload:
    data: str
    reply_routing_key: str
    output_format: str = "png"


async def extract_payload(msg):
    try:
        data = json.loads(msg.data)
        logger.debug(f"Received JSON data: {data}")
        payload = DrawioPayload(**data)
        return payload
    except json.JSONDecodeError as e:
        logger.exception("JSON decode error: %s", e)
        raise


class DrawioConverter:

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
        routing_key = DRAWIO_PROCESS_ROUTING_KEY
        stream = DRAWIO_PROCESS_STREAM
        logger.debug(f"Subscribing to routing_key: '{routing_key}' on stream '{stream}'")
        config = ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            max_deliver=1,
        )
        self.subscription = await self.jetstream.subscribe(
            subject=routing_key, queue=QUEUE_GROUP, stream=stream, config=config
        )
        logger.info(f"Subscribed to routing_key: '{routing_key}' on stream '{stream}'")

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
            result = await self.process_drawio_file(payload)
            logger.debug(f"Raw result: {len(result)} bytes")
            encoded_result = b64encode(result)
            logger.debug(f"Result: {len(result)} bytes: {encoded_result[:20]}")
            response = json.dumps({"result": encoded_result.decode("utf-8")})
            await self.publish_response(payload.reply_routing_key, response)
        except Exception as e:
            logger.exception(f"Error while processing DrawIO file: {e}", exc_info=e)
            await self.jetstream.publish(
                subject=payload.reply_routing_key,
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

    async def process_drawio_file(self, data: DrawioPayload) -> bytes:
        with TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "input.drawio"
            output_path = Path(tmp_dir) / f"output.{data.output_format}"
            with open(input_path, "w") as f:
                f.write(data.data)
            with open(output_path, "wb") as f:
                f.write(b"")
            await self.convert_drawio(input_path, output_path, data.output_format)
            return output_path.read_bytes()

    @staticmethod
    async def convert_drawio(input_path: Path, output_path: Path, output_format: str):
        logger.debug(f"Converting {input_path} to {output_path}")
        # Base command
        cmd = [
            "drawio",
            "--no-sandbox",
            "--export",
            input_path.as_posix(),
            "--format",
            output_format,
            "--output",
            output_path.as_posix(),
            "--border",
            "20",
        ]

        # Format-specific options
        if output_format == "png":
            cmd.extend(["--scale", "3"])  # Increase resolution (roughly 300 DPI)
        elif output_format == "svg":
            cmd.append("--embed-svg-images")  # Embed fonts in SVG

        env = os.environ.copy()
        env["DISPLAY"] = ":99"

        logger.debug("Creating subprocess...")
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        logger.debug("Waiting for conversion to complete...")
        stdout, stderr = await process.communicate()

        logger.debug(f"Return code: {process.returncode}")
        logger.debug(f"stdout: {stdout.decode()}")
        logger.debug(f"stderr: {stderr.decode()}")
        if process.returncode == 0:
            logger.info(f"Converted {input_path} to {output_path}")
            # If the output is SVG, optimize it and embed the font
            # if output_format.lower() == "svg":
            #     await DrawioConverter.optimize_svg(output_path)
        else:
            logger.error(f"Error converting {input_path}: {stderr.decode()}")

    @staticmethod
    async def optimize_svg(output_path):
        optimize_cmd = [
            "svgo",
            "-i",
            output_path.as_posix(),
            "-o",
            output_path.as_posix(),
        ]
        optimize_process = await asyncio.create_subprocess_exec(
            *optimize_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        optimize_stdout, optimize_stderr = await optimize_process.communicate()
        if optimize_process.returncode == 0:
            logger.info(f"Optimized SVG: {output_path}")
        else:
            logger.error(
                f"Error optimizing SVG {output_path}: {optimize_stderr.decode()}"
            )


if __name__ == "__main__":
    converter = DrawioConverter()
    asyncio.run(converter.run())
