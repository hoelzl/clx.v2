import asyncio

from nb.notebook_processor import NotebookProcessor, OutputSpec
from nb.output_spec import CompletedOutput


async def test_notebook_processor():
    output_spec = CompletedOutput(
        prog_lang="python",
        lang="en",
        notebook_format="html",
    )
    nbp = NotebookProcessor(output_spec)
    result = await nbp.process_notebook("# %%\nprint('Hello, world!')")
    print(result["result"])


if __name__ == "__main__":
    asyncio.run(test_notebook_processor())
