import asyncio
import json
import logging
import os
from base64 import b64decode
from typing import TYPE_CHECKING

import nats

from clx.utils.text_utils import sanitize_subject_name

if TYPE_CHECKING:
    from clx.file_ops import ConvertFileOperation

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
logger = logging.getLogger(__name__)


async def process_image_request(
    nc, op: "ConvertFileOperation", service: str, nats_subject: str
):
    try:
        reply_subject = _reply_subject_for_operation(op)
        sub = await _subscribe_to_nats_subject(nc, reply_subject)
        payload = {
            "data": op.input_file.path.read_text(),
            "reply_subject": reply_subject,
            "output_format": "png",
        }
        logger.debug(f"{service}: Sending Request to {nats_subject} with reply "
                     f"subject {reply_subject}")
        await nc.publish(nats_subject, json.dumps(payload).encode())
        msg = await _wait_for_processed_image_msg(service, sub)
        logger.debug(f"{service}: Received reply: {msg.data[:40]}")
        result = json.loads(msg.data.decode())
        if isinstance(result, dict):
            if img_base64 := result.get("result").encode():
                logger.debug(
                    f"{service}: Image data: len = {len(img_base64)}, {img_base64[:20]}"
                )
                img = b64decode(img_base64)
                logger.debug(f"{service}: Writing PNG data to {op.output_file}")
                op.output_file.write_bytes(img)
    except Exception as e:
        logger.exception("%s: Error %s", service, e)


def _reply_subject_for_operation(file: "ConvertFileOperation"):
    return sanitize_subject_name(f"img.completed.{file.input_file.relative_path}")


async def _subscribe_to_nats_subject(nc: nats.NATS, nats_subject: str):
    try:
        sub = await nc.subscribe(nats_subject)
        await nc.flush()
    except Exception as e:
        logger.exception(
            "Error while subscribing to nats topic '%s': '%s'", nats_subject, e
        )
        raise
    return sub


async def _wait_for_processed_image_msg(service, sub):
    logger.debug(f"{service}: Waiting for image")
    while True:
        try:
            msg = await sub.next_msg(timeout=1)
            logger.debug(f"{service}: Received image")
            return msg
        except asyncio.TimeoutError:
            continue
