import asyncio
import io
import json
import os
from base64 import b64decode, b64encode
from pprint import pprint
from tempfile import NamedTemporaryFile

import nats
from PIL import Image

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

plantuml_data = """\
@startuml
Alice -> Bob: Authentication Request
Bob --> Alice: Authentication Response
@enduml
"""

payload = {
    "data": plantuml_data,
}


async def test_plantuml_processor():
    nc = await nats.connect(NATS_URL)
    reply = await nc.request("plantuml.process", json.dumps(payload).encode(), timeout=60)
    result = json.loads(reply.data.decode())
    if isinstance(result, dict):
        if png_base64 := result.get("result").encode():
            print(f"PNG data: len = {len(png_base64)}, {png_base64[:20]}")
            png = b64decode(png_base64)
            with NamedTemporaryFile(suffix=".png", delete=False) as f:
                f.write(png)
                im = Image.open(f.name)
                im.show()
        else:
            pprint(result)
    else:
        print(result)

if __name__ == "__main__":
    asyncio.run(test_plantuml_processor())
