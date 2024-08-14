import asyncio
import datetime
import os

import click
import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def print_message(msg):
    print(f"{msg.subject}: {msg.data.decode()}")


async def main(n: int, m: int, t: float):
    for i in range(1, n + 1):
        nc = await nats.connect(NATS_URL)
        try:
            await nc.flush()
            await nc.subscribe("test.subject", cb=print_message)
            await nc.flush()
            for j in range(1, m + 1):
                await nc.publish(
                    "test.subject",
                    f"Message {i}:{j} at {datetime.datetime.now()}".encode(
                        "utf-8"),
                )
        finally:
            await nc.flush()
            await nc.close()
        await asyncio.sleep(t)


@click.command()
@click.option("-n", type=int, default=2, help="Number of times to connect")
@click.option("-m", type=int, default=2, help="Number of messages to send")
@click.option("-t", type=float, default=0.5, help="Time to wait after each run")
def run_main(n: int, m: int, t: float):
    asyncio.run(main(n, m, t))


if __name__ == "__main__":
    run_main()
