import logging
import re
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

from attrs import frozen, field

from clx.utils.text_utils import as_dir_name, sanitize_file_name

if TYPE_CHECKING:
    from clx.course import Course

logger = logging.getLogger(__name__)

SLIDES_PREFIX = "slides_"

SKIP_DIRS_FOR_COURSE = frozenset(
    (
        "__pycache__",
        ".git",
        ".ipynb_checkpoints",
        ".mypy_cache",
        ".pytest_cache",
        ".tox",
        ".venv",
        ".vs",
        ".vscode",
        ".idea",
        "build",
        "dist",
        ".cargo",
        ".idea",
        ".vscode",
        "target",
        "out",
        "CMakeFiles",
    )
)

SKIP_DIRS_FOR_OUTPUT = SKIP_DIRS_FOR_COURSE | frozenset({"pu", "drawio"})

PLANTUML_EXTENSIONS = frozenset({".pu", ".puml", ".plantuml"})

SUPPORTED_PROG_LANG_EXTENSIONS = frozenset(
    (
        ".py",
        ".cpp",
        ".c",
        ".rust",
        ".rs",
        ".java",
        ".cs",
        ".md",
    )
)

IGNORE_PATH_REGEX = re.compile(r"(.*\.egg-info.*|.*cmake-build-.*)")


def is_slides_file(input_path: Path) -> bool:
    return (
        input_path.name.startswith(SLIDES_PREFIX)
        and input_path.suffix in SUPPORTED_PROG_LANG_EXTENSIONS
    )


def is_ignored_dir_for_course(dir_path: Path) -> bool:
    for part in dir_path.parts:
        if part in SKIP_DIRS_FOR_COURSE:
            return True
        if re.match(IGNORE_PATH_REGEX, part):
            return True
    return False


def is_ignored_dir_for_output(dir_path: Path) -> bool:
    for part in dir_path.parts:
        if part in SKIP_DIRS_FOR_OUTPUT:
            return True
        if re.match(IGNORE_PATH_REGEX, part):
            return True
    return False


def simplify_ordered_name(name: str, prefix: str | None = None) -> str:
    parts = name.split("_")
    if prefix:
        assert parts[0] == prefix
    return "_".join(parts[2:])


class Lang(StrEnum):
    DE = "de"
    EN = "en"


class Format(StrEnum):
    HTML = "html"
    NOTEBOOK = "notebook"
    CODE = "code"


class Mode(StrEnum):
    CODE_ALONG = "code-along"
    COMPLETED = "completed"


def ext_for(format_: str | Format) -> str:
    match str(format_):
        case "html":
            return ".html"
        case "notebook":
            return ".ipynb"
        case "code":
            return ".py"
        case _:
            raise ValueError(f"Unknown format: {format_}")


@frozen
class OutputSpec:
    course: "Course"
    lang: str = field(converter=str)
    format: str = field(converter=str)
    mode: str = field(converter=str)
    root_dir: Path
    output_dir: Path = field(init=False)

    def __attrs_post_init__(self):
        lang = as_dir_name(self.lang, self.lang)
        format_ = as_dir_name(self.format, self.lang)
        mode = as_dir_name(self.mode, self.lang)
        object.__setattr__(
            self,
            "output_dir",
            self.root_dir
            / f"{lang}"
            / sanitize_file_name(self.course.name[self.lang])
            / f"{format_}/{mode}",
        )

    def __iter__(self):
        return iter((self.lang, self.format, self.mode, self.output_dir))


def output_specs(course: "Course", root_dir: Path) -> OutputSpec:
    for lang_dir in [Lang.DE, Lang.EN]:
        for format_dir in [Format.HTML, Format.NOTEBOOK]:
            for mode_dir in [Mode.CODE_ALONG, Mode.COMPLETED]:
                yield OutputSpec(
                    course=course,
                    lang=lang_dir,
                    format=format_dir,
                    mode=mode_dir,
                    root_dir=root_dir,
                )
    for lang_dir in [Lang.DE, Lang.EN]:
        yield OutputSpec(
            course=course,
            lang=lang_dir,
            format=Format.CODE,
            mode=Mode.COMPLETED,
            root_dir=root_dir,
        )
