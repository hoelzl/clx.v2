import asyncio
import logging
import os
import platform
import warnings
from hashlib import sha3_224
from pathlib import Path

import jupytext.config as jupytext_config
import traitlets.log
from jinja2 import Environment, PackageLoader, StrictUndefined
from jupytext import jupytext
from nbconvert import HTMLExporter
from nbconvert.preprocessors import ExecutePreprocessor
from nbformat import NotebookNode
from nbformat.validator import normalize

from .payload import NotebookPayload
from .output_spec import OutputSpec
from .utils.jupyter_utils import (
    Cell,
    get_cell_type,
    get_slide_tag,
    get_tags,
    is_answer_cell,
    is_code_cell,
    is_markdown_cell,
    warn_on_invalid_code_tags,
    warn_on_invalid_markdown_tags,
)
from .utils.prog_lang_utils import (
    jinja_prefix_for,
    jupytext_format_for,
    kernelspec_for,
    language_info,
)


def string_to_list(string: str) -> list[str]:
    return [s.strip() for s in string.split(",")]


# Configuration
JINJA_LINE_STATEMENT_PREFIX = os.environ.get("JINJA_LINE_STATEMENT_PREFIX", "# j2")
JINJA_TEMPLATES_PATH = os.environ.get("JINJA_TEMPLATES_PATH", "templates")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_CELL_PROCESSING = os.environ.get("LOG_CELL_PROCESSING", "False") == "True"

# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s - notebook-processor - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class CellIdGenerator:
    def __init__(self):
        self.unique_ids: set[str] = set()
        self.id_uniquifier: int = 1

    def set_cell_id(self, cell: Cell, index: int) -> None:
        cell_hash = sha3_224()
        cell_source: str = cell["source"]
        hash_text = cell_source
        while True:
            cell_hash.update(hash_text.encode("utf-8"))
            cell_id = cell_hash.hexdigest()[:16]
            if cell_id in self.unique_ids:
                hash_text = f"{index}:{cell_source}"
                index += 1
            else:
                self.unique_ids.add(cell_id)
                cell.id = cell_id
                break


class DontWarnForMissingAltTags(logging.Filter):
    def filter(self, record):
        return "Alternative text is missing" not in record.getMessage()


class NotebookProcessor:
    def __init__(self, output_spec: OutputSpec):
        self.output_spec = output_spec
        self.id_generator = CellIdGenerator()

    async def process_notebook(self, payload: NotebookPayload):
        logger.info(f"Processing notebook '{payload.notebook_path}'")
        expanded_nb = await self.load_and_expand_jinja_template(
            payload.notebook_text, payload.notebook_path
        )
        processed_nb = self.process_notebook_for_spec(expanded_nb, payload)
        result = await self.create_contents(processed_nb, payload)
        logger.debug(f"Processed notebook. Result: {result[:100]}...")
        return result

    async def load_and_expand_jinja_template(
        self, notebook_text: str, notebook_file: str
    ) -> str:
        logger.debug("Loading and expanding Jinja template")
        jinja_env = self._create_jinja_environment()
        nb_template = jinja_env.from_string(
            notebook_text,
            globals=self._create_jinja_globals(self.output_spec),
        )
        logger.debug(f"Jinja template created for {notebook_file}")
        expanded_nb = await nb_template.render_async()
        logger.debug(f"Jinja template expanded for {notebook_file}")
        return expanded_nb

    def _create_jinja_environment(self):
        templates_path = f"{JINJA_TEMPLATES_PATH}_{self.output_spec.prog_lang}"
        logger.debug(f"Creating Jinja environment with templates from {templates_path}")
        jinja_env = Environment(
            loader=(PackageLoader("nb", templates_path)),
            autoescape=False,
            undefined=StrictUndefined,
            line_statement_prefix=jinja_prefix_for(self.output_spec.prog_lang),
            keep_trailing_newline=True,
            enable_async=True,
        )
        logger.debug("Jinja environment created")
        return jinja_env

    @staticmethod
    def _create_jinja_globals(output_spec):
        return {
            "is_notebook": output_spec.notebook_format == "notebook",
            "is_html": output_spec.notebook_format == "html",
            "lang": output_spec.lang,
        }

    def process_notebook_for_spec(
        self, expanded_nb: str, payload: NotebookPayload
    ) -> NotebookNode:
        jupytext_format = jupytext_format_for(self.output_spec.prog_lang)
        logger.debug(
            f"Processing notebook for in format "
            f"'{self.output_spec.notebook_format}' with Jupytext format "
            f"'{jupytext_format}'"
        )
        nb = jupytext.reads(expanded_nb, fmt=jupytext_format)
        processed_nb = self._process_notebook_node(nb, payload)
        return processed_nb

    def _process_notebook_node(
        self, nb: NotebookNode, payload: NotebookPayload
    ) -> NotebookNode:
        new_cells = [
            self._process_cell(cell, index, payload)
            for index, cell in enumerate(nb.get("cells", []))
            if self.output_spec.is_cell_included(cell)
        ]
        nb.cells = new_cells
        nb.metadata["language_info"] = language_info(payload.prog_lang)
        nb.metadata["kernelspec"] = kernelspec_for(payload.prog_lang)
        _, normalized_nb = normalize(nb)
        return normalized_nb

    def _process_cell(self, cell: Cell, index: int, payload: NotebookPayload) -> Cell:
        self._generate_cell_metadata(cell, index)
        if LOG_CELL_PROCESSING:
            logger.debug(f"Processing cell {cell} of {payload.notebook_path}")
        if is_code_cell(cell):
            return self._process_code_cell(cell)
        elif is_markdown_cell(cell):
            return self._process_markdown_cell(cell)
        else:
            logger.warning(f"Keeping unknown cell type {get_cell_type(cell)!r}.")
            return cell

    def _generate_cell_metadata(self, cell, index):
        self.id_generator.set_cell_id(cell, index)
        self._process_slide_tag(cell)

    @staticmethod
    def _process_slide_tag(cell):
        slide_tag = get_slide_tag(cell)
        if slide_tag:
            cell["metadata"]["slideshow"] = {"slide_type": slide_tag}

    def _process_code_cell(self, cell: Cell):
        if not self.output_spec.is_cell_contents_included(cell):
            cell["source"] = ""
            cell["outputs"] = []
        warn_on_invalid_code_tags(get_tags(cell))
        return cell

    def _process_markdown_cell(self, cell: Cell):
        tags = get_tags(cell)
        warn_on_invalid_markdown_tags(tags)
        self._process_markdown_cell_contents(cell)
        return cell

    def _process_markdown_cell_contents(self, cell: Cell):
        tags = get_tags(cell)
        if "notes" in tags:
            contents = cell["source"]
            cell["source"] = "<div style='background:yellow'>\n" + contents + "\n</div>"
        if is_answer_cell(cell):
            answer_text = "Answer" if self.output_spec.lang == "en" else "Antwort"
            prefix = f"*{answer_text}:* "
            if self.output_spec.is_cell_contents_included(cell):
                cell["source"] = prefix + cell["source"]
            else:
                cell["source"] = prefix

    async def create_contents(
        self, processed_nb: NotebookNode, payload: NotebookPayload
    ) -> str:
        try:
            if self.output_spec.notebook_format == "html":
                result = await self._create_using_nbconvert(processed_nb, payload)
            else:
                result = await self._create_using_jupytext(processed_nb)
            return result
        except RuntimeError as e:
            logging.exception("Failed to convert notebook %s to HTML: %s", e)

    async def _create_using_nbconvert(self, processed_nb, payload: NotebookPayload):
        traitlets.log.get_logger().addFilter(DontWarnForMissingAltTags())
        if self.output_spec.evaluate_for_html:
            if any(is_code_cell(cell) for cell in processed_nb.get("cells", [])):
                logger.debug(
                    "Evaluating and writing notebook " f"{payload.notebook_path}"
                )
                try:
                    # To silence warnings about frozen modules...
                    os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"
                    with warnings.catch_warnings():
                        warnings.filterwarnings(
                            "ignore",
                            "Proactor event loop does not implement add_reader",
                        )
                        ep = ExecutePreprocessor(timeout=None)
                        loop = asyncio.get_running_loop()
                        path = (
                            Path("C:/tmp")
                            if platform.system() == "Windows"
                            else Path("/tmp")
                        )
                        for extra_file, contents in payload.other_files.items():
                            logger.debug(f"Writing extra file {extra_file}")
                            (path / extra_file).write_text(contents)
                        await loop.run_in_executor(
                            None,
                            lambda: ep.preprocess(
                                processed_nb,
                                resources={"metadata": {"path": path}},
                            ),
                        )
                except Exception as e:
                    logger.exception(
                        f"Error while processing notebook %s: %s",
                        payload.notebook_path,
                        e,
                    )
                    raise
            else:
                logger.debug(
                    f"Notebook {payload.notebook_path} contains " "no code cells."
                )
        html_exporter = HTMLExporter(template_name="classic")
        (body, _resources) = html_exporter.from_notebook_node(processed_nb)
        return body

    async def _create_using_jupytext(self, processed_nb):
        config = jupytext_config.JupytextConfiguration(
            notebook_metadata_filter="-all", cell_metadata_filter="-all"
        )
        output = jupytext.writes(
            processed_nb,
            fmt=self.output_spec.jupytext_format,
            config=config,
        )
        if not output.endswith("\n"):
            output += "\n"
        return output
