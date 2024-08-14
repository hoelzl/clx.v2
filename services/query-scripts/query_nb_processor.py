import asyncio
import json
import os
from pprint import pprint
import webbrowser
from tempfile import NamedTemporaryFile

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

payload = {
    "prog_lang": "python",
    "language": "en",
    "notebook_format": "html",
    "output_type": "completed",
    "notebook_text": "# %%\nprint('Hello, world!')"
}


async def test_notebook_processor():
    nc = await nats.connect(NATS_URL)
    reply = await nc.request("nb.process", json.dumps(payload).encode(), timeout=5)
    result = json.loads(reply.data.decode())
    if isinstance(result, dict):
        if notebook := result.get("result"):
            print(notebook)
            with NamedTemporaryFile(suffix=".html", delete=False) as f:
                f.write(notebook.encode())
                webbrowser.open(f.name)
        else:
            pprint(result)
    else:
        print(result)

if __name__ == "__main__":
    asyncio.run(test_notebook_processor())