import asyncio
import logging
from pathlib import Path

import click
from clx.course import Course
from clx.course_spec import CourseSpec


logging.getLogger().setLevel(logging.INFO)


async def main(spec_file, data_dir, output_dir):
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


@click.command()
@click.argument(
    "spec-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
)
@click.option(
    "--output-dir",
    type=click.Path(exists=False, file_okay=False, dir_okay=True, path_type=Path),
)
def run_main(spec_file, data_dir, output_dir):
    asyncio.run(main(spec_file, data_dir, output_dir))


if __name__ == "__main__":
    run_main()
