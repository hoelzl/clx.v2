import asyncio
import json
import logging
import os
from base64 import b64decode
from typing import ClassVar

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
logger = logging.getLogger(__name__)


async def connect_client_with_retry(nats_url: str, num_retries: int = 5):
    for i in range(num_retries):
        try:
            logger.debug(f"Trying to connect to NATS at {nats_url}")
            nc = await nats.connect(nats_url)
            logger.info(f"Connected to NATS at {nats_url}")
            return nc
        except Exception as e:
            logger.exception("Error connecting to NATS: %s", e)
            await asyncio.sleep(0.25 * 2**i)
    raise OSError("Could not connect to NATS")


async def process_image_request(file, service: str, nats_topic: str, timeout=120):
    try:
        nc = file.nats_connection
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
        logger.exception("%s: Error %s", service, e)
