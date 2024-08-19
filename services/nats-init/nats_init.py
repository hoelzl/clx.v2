import asyncio
import logging
import os

import nats
from nats.js.api import RetentionPolicy
from nats.js.errors import NotFoundError


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222")

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - nats-init - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


FORCE_DELETE_STREAMS = os.environ.get("FORCE_DELETE_STREAMS", True)

# TODO: These should be shared between services and clx.utils.nats_utils
NATS_STREAMS = {
    "drawio_process_stream": {
        "stream_name": "DRAWIO_PROCESS_STREAM",
        "subjects": ["drawio.process", "drawio.process.>"],
        "retention": RetentionPolicy.WORK_QUEUE,
    },
    "plantuml_process_stream": {
        "stream_name": "PLANTUML_PROCESS_STREAM",
        "subjects": ["plantuml.process", "plantuml.process.>"],
        "retention": RetentionPolicy.WORK_QUEUE,
    },
    "img_result_stream": {
        "stream_name": "IMG_RESULT_STREAM",
        "subjects": ["img.result", "img.result.>"],
        "retention": RetentionPolicy.WORK_QUEUE,
    },
    "notebook_process_stream": {
        "stream_name": "NOTEBOOK_PROCESS_STREAM",
        "subjects": ["notebook.process", "notebook.process.>"],
        "retention": RetentionPolicy.WORK_QUEUE,
    },
    "notebook_result_stream": {
        "stream_name": "NOTEBOOK_RESULT_STREAM",
        "subjects": ["notebook.result", "notebook.result.>"],
        "retention": RetentionPolicy.WORK_QUEUE,
    },
}


async def create_streams():
    nc = await nats.connect(NATS_URL)

    try:
        logger.info(f"Connected to NATS at {NATS_URL}")
        js = nc.jetstream()
        for stream in NATS_STREAMS.values():
            try:
                await create_stream(js, **stream)
            except Exception as e:
                logger.exception(f"Error creating stream {stream}: {e}", exc_info=e)
    except Exception as e:
        logger.error(f"Error connecting to NATS: {e}")
    finally:
        await nc.close()
        logger.info("NATS connection closed")


async def create_stream(js, stream_name, subjects, **kwargs):
    try:
        if FORCE_DELETE_STREAMS:
            try:
                logger.info(f"Force-deleting {stream_name} stream")
                await js.delete_stream(stream_name)
                logger.debug(f"Deleted {stream_name} stream")
            except NotFoundError:
                logger.info(f"{stream_name} stream does not exist")
        for i in range(5):
            try:
                logger.debug(f"Trying to determine if stream {stream_name}  exists")
                if await does_stream_exist(js, stream_name):
                    logger.debug(f"Stream {stream_name} exists")
                    return
                logger.debug(f"Stream {stream_name} does not exist, trying to create")
                await js.add_stream(
                    name=stream_name,
                    subjects=subjects,
                    **kwargs,
                )
                logger.info(f"{stream_name} stream created successfully")
                break
            except TimeoutError as e:
                logger.info(f"Timeout creating {stream_name} stream: {e}")
                await asyncio.sleep(1)
            except NotFoundError as e:
                logger.info(
                    f"No NATS server found when creating stream {stream_name}: {e}"
                )
            await asyncio.sleep(i)
    except Exception as e:
        logger.error(f"Error creating {stream_name} stream: {e}")


async def does_stream_exist(js, name):
    try:
        await js.stream_info(name)
        logger.debug(f"{name} stream already exists")
        return True
    except Exception as e:
        logger.debug(f"{name} stream does not exist: {e}")
    return False


async def main():
    logger.info("Starting NATS initialization service")
    await create_streams()
    logger.info("NATS initialization complete. Shutting down initialization service")


if __name__ == "__main__":
    asyncio.run(main())
