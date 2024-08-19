import asyncio
import json
import logging
import os
from base64 import b64decode
from typing import TYPE_CHECKING

import nats
from nats import NATS
from nats.js import JetStreamContext

from clx.utils.text_utils import sanitize_subject_name

if TYPE_CHECKING:
    from clx.file_ops import ConvertFileOperation

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
logger = logging.getLogger(__name__)

NATS_STREAMS = {
    "drawio_process_stream": {
        "stream_name": "DRAWIO_PROCESS_STREAM",
        "subject": "drawio.process",
    },
    "plantuml_process_stream": {
        "stream_name": "PLANTUML_PROCESS_STREAM",
        "subject": "plantuml.process",
    },
    "img_result_stream": {
        "stream_name": "IMG_RESULT_STREAM",
        "subject": "img.result",
    },
    "notebook_process_stream": {
        "stream_name": "NOTEBOOK_PROCESS_STREAM",
        "subject": "notebook.process",
    },
    "notebook_result_stream": {
        "stream_name": "NOTEBOOK_RESULT_STREAM",
        "subject": "notebook.result",
    },
}
IMG_RESULT_STREAM = NATS_STREAMS["img_result_stream"]

reply_counter = 0


async def process_image_request(
    op: "ConvertFileOperation", service: str, nats_stream_key: str
):
    nats_stream_info = NATS_STREAMS[nats_stream_key]
    nats_subject: str = nats_stream_info["subject"]
    nats_stream_name = nats_stream_info["stream_name"]

    nc: NATS = await nats.connect(NATS_URL)
    js: JetStreamContext = nc.jetstream()
    try:
        reply_subject, reply_stream = _reply_subject_and_stream_for_operation(op)
        psub = await _subscribe_to_nats_subject(
            nc, js, service, reply_subject, reply_stream
        )
        payload = {
            "data": op.input_file.path.read_text(),
            "reply_subject": reply_subject,
            "output_format": "png",
        }
        logger.debug(
            f"{service}: Sending Request to {nats_subject} on stream "
            f"{nats_stream_name} with reply subject {reply_subject}"
        )
        await js.publish(
            subject=nats_subject,
            stream=nats_stream_name,
            payload=json.dumps(payload).encode(),
        )
        logger.debug(
            f"{service}: Published to subject '{nats_subject}' on "
            f"stream '{nats_stream_name}', waiting for response"
        )
        msg = await _wait_for_processed_image_msg(service, psub)
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
        logger.exception(f"{service}: Error {e}")
    finally:
        await nc.close()
        logger.debug(f"{service}: Cleaned up")


def _reply_subject_and_stream_for_operation(file: "ConvertFileOperation"):
    global reply_counter
    reply_counter += 1
    return (
        sanitize_subject_name(
            f"img.result.{file.input_file.relative_path}_{reply_counter}"
        ),
        IMG_RESULT_STREAM["stream_name"],
    )


async def _subscribe_to_nats_subject(
    nc: nats.NATS, js: JetStreamContext, service, nats_subject: str, nats_stream: str
):
    try:
        logger.debug(
            f"{service}: Subscribing to subject '{nats_subject}' on stream "
            f"'{nats_stream}'"
        )
        psub = await js.pull_subscribe(subject=nats_subject, stream=nats_stream)
        await nc.flush()
        logger.debug(
            f"{service}: Subscribed to reply subject '{nats_subject}' on stream "
            f"'{nats_stream}'"
    )
    except Exception as e:
        logger.exception(
            f"{service}: Error while subscribing to nats subject "
            f"'{nats_subject}': {e}"
        )
        raise
    return psub


async def _wait_for_processed_image_msg(service, psub):
    while True:
        try:
            logger.debug(f"{service}: Waiting for image data")
            msgs = await psub.fetch(1)
            if len(msgs) == 0:
                raise ValueError("No image received")
            for msg in msgs:
                logger.debug(f"{service}: Received message, sending ack: {msg}")
                await msg.ack()
            logger.debug(f"{service}: Received {len(msgs)} image(s)")
            return msgs[0]
        except asyncio.TimeoutError:
            continue
