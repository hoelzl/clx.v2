import asyncio
import json
import logging
import os
from base64 import b64encode
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import nats

# Configuration
NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
QUEUE_GROUP = os.environ.get("DRAWIO_CONVERTER_QUEUE_GROUP", "DRAWIO_CONVERTER")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - drawio-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class DrawioPayload:
    data: str
    reply_subject: str
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

    async def connect_nats(self):
        try:
            self.nats_client = await nats.connect(NATS_URL)
            logger.info(f"Connected to NATS at {NATS_URL}")
        except Exception as e:
            logger.exception("Error connecting to NATS: %s", e)
            raise

    async def subscribe_to_events(self):
        subject = f"drawio.process"
        queue = QUEUE_GROUP
        logger.debug(f"Subscribing to subject: {subject} on queue group {queue}")
        await self.nats_client.subscribe(
            subject,
            cb=self.handle_event,
            queue=queue,
        )
        await self.nats_client.flush()
        logger.info(f"Subscribed to subject: {subject} on queue group {queue}")

    async def handle_event(self, msg):
        payload = await extract_payload(msg)
        try:
            result = await self.process_drawio_file(payload)
            logger.debug(f"Raw result: {len(result)} bytes")
            encoded_result = b64encode(result)
            logger.debug(f"Result: {len(result)} bytes: {encoded_result[:20]}")
            response = json.dumps({"result": encoded_result.decode("utf-8")})
            await self.nats_client.publish(
                payload.reply_subject, response.encode("utf-8")
            )
        except Exception as e:
            logger.exception("Error while handling event: %s", e)
            await self.nats_client.publish(
                payload.reply_subject, json.dumps({"error": str(e)}).encode("utf-8")
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

    async def run(self):
        await self.connect_nats()
        await self.subscribe_to_events()
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt, shutting down...")
        finally:
            if self.nats_client:
                await self.nats_client.close()


if __name__ == "__main__":
    converter = DrawioConverter()
    asyncio.run(converter.run())
