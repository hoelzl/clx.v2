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
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")
QUEUE_GROUP = os.environ.get("QUEUE_GROUP", "PLANTUML_CONVERTER")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - plantuml-converter - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@dataclass
class PlantUmlPayload:
    data: str
    output_format: str = "png"


class PlantUmlConverter:
    def __init__(self):
        self.nats_client: nats.NATS | None = None

    async def connect_nats(self):
        try:
            self.nats_client = await nats.connect(NATS_URL)
            logger.info(f"Connected to NATS at {NATS_URL}")
        except Exception as e:
            logger.error(f"Error connecting to NATS: {e}")
            raise

    async def subscribe_to_events(self):
        subject = f"plantuml.process"
        queue = QUEUE_GROUP
        logger.debug(f"Subscribing to subject: {subject} on queue group {queue}")
        await self.nats_client.subscribe(
            subject,
            cb=self.handle_event,
            queue=queue,
        )
        logger.info(f"Subscribed to subject: {subject} on queue group {queue}")

    async def handle_event(self, msg):
        try:
            data = json.loads(msg.data)
            logger.debug(f"Received JSON data: {data}")
            result = await self.try_to_process_payload(data)
            encoded_result = b64encode(result)
            logger.debug(f"Result: {encoded_result[:20]}")
            response = json.dumps({"result": encoded_result.decode("utf-8")})
            await msg.respond(response.encode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            await msg.respond(
                json.dumps({"error": f"JSON decode error {e}"}).encode("utf-8")
            )
        except Exception as e:
            logger.error(f"Error while handling event: {e}")
            await msg.respond(json.dumps({"error": str(e)}).encode("utf-8"))

    async def try_to_process_payload(self, data):
        try:
            logger.debug(f"Processing payload: {data}")
            payload = PlantUmlPayload(**data)
            return await self.process_plantuml_file(payload)
        except TypeError as e:
            logger.error(f"Error: {e}")
            raise

    async def process_plantuml_file(self, data: PlantUmlPayload) -> bytes:
        logger.debug(f"Processing PlantUML file: {data}")
        with TemporaryDirectory() as tmp_dir:
            input_path = Path(tmp_dir) / "plantuml.pu"
            output_path = input_path.with_suffix(f".{data.output_format}")
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
    converter = PlantUmlConverter()
    asyncio.run(converter.run())
