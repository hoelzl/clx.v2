import asyncio
import logging
import os
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

import click
from clx.course import Course
from clx.course_spec import CourseSpec

logging.getLogger().setLevel(logging.INFO)


class FileEventHandler(PatternMatchingEventHandler):
    def __init__(self, course, data_dir, loop, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course = course
        self.data_dir = data_dir
        self.loop = loop

    def on_created(self, event):
        self.loop.create_task(
            self.handle_event(self.course.on_file_created, Path(event.src_path))
        )

    def on_moved(self, event):
        self.loop.create_task(
            self.handle_event(
                self.course.on_file_moved, Path(event.src_path), Path(event.dest_path)
            )
        )

    def on_deleted(self, event):
        self.loop.create_task(
            self.handle_event(self.course.on_file_deleted, Path(event.src_path))
        )

    def on_modified(self, event):
        self.loop.create_task(
            self.handle_event(self.course.process_file, Path(event.src_path))
        )

    async def handle_event(self, method, *args):
        try:
            await method(*args)
        except Exception as e:
            logging.error(f"Error handling event: {e}")


async def main(spec_file, data_dir, output_dir, watch):
    if data_dir is None:
        data_dir = spec_file.parents[1]
        logging.debug(f"Data directory set to {data_dir}")
        assert data_dir.exists(), f"Data directory {data_dir} does not exist."
    if output_dir is None:
        output_dir = data_dir / "output"
        output_dir.mkdir(exist_ok=True)
        logging.debug(f"Output directory set to {output_dir}")
    spec = CourseSpec.from_file(spec_file)
    course = Course.from_spec(spec, data_dir, output_dir)
    await course.process_all()

    if watch:
        loop = asyncio.get_event_loop()
        event_handler = FileEventHandler(course, data_dir, loop, patterns=["*"])
        observer = Observer()
        observer.schedule(event_handler, str(data_dir), recursive=True)
        observer.start()
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()


@click.command()
@click.argument(
    "spec-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--data-dir",
    "-d",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--watch",
    "-w",
    is_flag=True,
    help="Watch for file changes and automatically process them.",
)
def run_main(spec_file, data_dir, output_dir, watch):
    asyncio.run(main(spec_file, data_dir, output_dir, watch))


if __name__ == "__main__":
    run_main()
