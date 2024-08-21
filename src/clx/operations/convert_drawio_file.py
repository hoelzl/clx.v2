import logging
from typing import Any

from attrs import frozen

from clx.operations.convert_file import ConvertFileOperation
from clx.utils.nats_utils import process_image_request

logger = logging.getLogger(__name__)


@frozen
class ConvertDrawIoFileOperation(ConvertFileOperation):
    async def exec(self, *_args, **_kwargs) -> Any:
        logger.info(
            f"Converting DrawIO file {self.input_file.relative_path} "
            f"to {self.output_file}"
        )
        await process_image_request(self, "DrawIO", "drawio_process_stream")
        self.input_file.generated_outputs.add(self.output_file)
