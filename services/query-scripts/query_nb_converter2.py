import asyncio

from nb.notebook_processor import NotebookProcessor
from nb.output_spec import CompletedOutput
from nb.payload import NotebookPayload


async def test_notebook_processor():
    output_spec = CompletedOutput(
        prog_lang="python",
        lang="en",
        notebook_format="html",
    )
    nbp = NotebookProcessor(output_spec)
    payload = NotebookPayload(
        notebook_text="# %%\nprint('Hello, world!')",
        notebook_path="test_notebook.py",
        reply_routing_key="notebook.result.test_notebook_1",
        prog_lang="python",
        language="en",
        notebook_format="html",
        output_type="completed",
        other_files={}
    )
    result = await nbp.process_notebook(payload)
    print(result["result"])


if __name__ == "__main__":
    asyncio.run(test_notebook_processor())
