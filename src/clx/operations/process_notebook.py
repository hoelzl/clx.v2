import asyncio
import json
import logging
import os
from asyncio import CancelledError
from pathlib import Path
from typing import Any

import nats
from attrs import frozen
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig

from clx.course_file import Notebook
from clx.operation import Operation
from clx.utils.path_utils import is_image_file, is_image_source_file
from clx.utils.text_utils import sanitize_key_name, unescape

logger = logging.getLogger(__name__)

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
NB_PROCESS_ROUTING_KEY = "notebook.process"
NB_PROCESS_STREAM = "NOTEBOOK_PROCESS_STREAM"
NB_RESULT_STREAM = "NOTEBOOK_RESULT_STREAM"
NB_RESULT_ROUTING_KEY = "notebook.result"


@frozen
class ProcessNotebookOperation(Operation):
    input_file: "Notebook"
    output_file: Path
    lang: str
    format: str
    mode: str
    prog_lang: str

    @property
    def reply_subject(self) -> str:
        id_ = self.input_file.topic.id
        num = self.input_file.number_in_section
        lang = self.lang
        format_ = self.format
        mode = self.mode
        routing_key_postfix = f"{id_}_{num}_{lang}_{format_}_{mode}"
        return sanitize_key_name(f"notebook.result.{routing_key_postfix}")

    async def exec(self, *args, **kwargs) -> Any:
        try:
            logger.info(
                f"Processing notebook '{self.input_file.relative_path}' "
                f"to '{self.output_file}'"
            )
            await self.process_request()
            self.input_file.generated_outputs.add(self.output_file)
        except Exception as e:
            logger.exception(
                f"Error while processing notebook {self.input_file.relative_path}: {e}"
            )
            raise

    async def process_request(self):
        logger.debug(
            f"Notebook-Processor: Processing request for "
            f"{self.input_file.relative_path}"
        )

        logger.debug(f"Notebook-Processor: Processing {self.input_file.relative_path} ")
        nc = await nats.connect(NATS_URL)
        try:
            js: JetStreamContext = nc.jetstream()
            sub = await self.subscribe_to_reply_subject(nc, js)
            await self.send_nb_process_msg(js)
            msg = await self.wait_for_processed_notebook_msg(sub)
            if msg is not None and msg.data:
                logger.debug(f"Notebook-Processor: Received  reply: {msg.data[:40]}")
                self.write_notebook_to_file(msg)
            else:
                logger.error(f"Notebook-Processor: Received error: {msg}")
        except Exception as e:
            logger.exception(
                "Notebook-Processor: Error while processing request: " "%s", e
            )
        finally:
            await nc.close()
            logger.debug("Notebook-Processor: Cleaned up")

    async def subscribe_to_reply_subject(self, nc: nats.NATS, js: JetStreamContext):
        try:
            logger.debug(
                f"Subscribing to subject '{self.reply_subject}' on stream "
                f"{NB_RESULT_STREAM}"
            )
            config = ConsumerConfig(
                ack_policy=AckPolicy.EXPLICIT,
                max_deliver=1,
            )
            sub = await js.subscribe(
                subject=self.reply_subject, stream=NB_RESULT_STREAM, config=config
            )
            await nc.flush()
            logger.debug(
                f"Subscribed to reply subject '{self.reply_subject}' on "
                f"stream {NB_RESULT_STREAM}"
            )
        except Exception as e:
            logger.exception(
                "Error while subscribing to reply subject "
                f"'{self.reply_subject}': {e}"
            )
            raise
        return sub

    async def send_nb_process_msg(self, js: JetStreamContext):
        payload = self.build_payload()
        logger.debug(f"Notebook-Processor: sending request: {payload}")
        for num_tries in range(10):
            try:
                await js.publish(
                    subject=NB_PROCESS_ROUTING_KEY,
                    stream=NB_PROCESS_STREAM,
                    payload=json.dumps(payload).encode(),
                )
                logger.debug(
                    f"Notebook-Processor: Published to subject "
                    f"{NB_PROCESS_ROUTING_KEY} on stream {NB_PROCESS_STREAM}, "
                    f"waiting for response"
                )
                break
            except CancelledError:
                logger.info(
                    f"Notebook-Processor: Timed out while publishing to subject "
                    f"{NB_PROCESS_ROUTING_KEY} on stream {NB_PROCESS_STREAM}, retrying"
                )
                continue
            except Exception as e:
                logger.exception(
                    "Error while publishing notebook '%s': '%s'", self.reply_subject, e
                )
                raise

    def build_payload(self):
        notebook_path = self.input_file.relative_path.name
        other_files = {
            str(file.relative_path): file.path.read_text()
            for file in self.input_file.topic.files
            if file != self.input_file
            and not is_image_file(file.path)
            and not is_image_source_file(file.path)
        }
        return {
            "notebook_text": self.input_file.path.read_text(),
            "notebook_path": notebook_path,
            "reply_subject": self.reply_subject,
            "prog_lang": self.prog_lang,
            "language": self.lang,
            "notebook_format": self.format,
            "output_type": self.mode,
            "other_files": other_files,
        }

    async def wait_for_processed_notebook_msg(self, sub):
        logger.debug(
            f"Notebook-Processor: Waiting for processed notebook {self.reply_subject}"
        )
        for _ in range(10):
            try:
                logger.debug(f"Waiting for notebook data")
                msg = await sub.next_msg(timeout=None)
                await msg.ack()
                logger.debug(f"Received {msg.data[:40]}")
                return msg
            except (TimeoutError, CancelledError):
                logger.debug(f"Timed out while waiting for processed notebook")
                await asyncio.sleep(1.0)
                continue
        raise TypeError("Wait timed out!")

    def write_notebook_to_file(self, msg):
        data = json.loads(msg.data.decode())
        logger.debug(f"Notebook-Processor: Decoded message {str(data)[:50]}")
        if isinstance(data, dict):
            if notebook := data.get("result"):
                logger.debug(
                    f"Notebook-Processor: Writing notebook to {self.output_file}"
                )
                if not self.output_file.parent.exists():
                    self.output_file.parent.mkdir(parents=True, exist_ok=True)
                self.output_file.write_text(notebook)
            elif error := data.get("error"):
                logger.error(f"Notebook-Processor: Error: {error}")
            else:
                logger.error(f"Notebook-Processor: No key 'result' in {unescape(data)}")
        else:
            logger.error(f"Notebook-Processor: Reply not a dict {unescape(data)}")
