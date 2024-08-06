import re
from enum import StrEnum
from pathlib import Path

from attrs import frozen, field

from clx.utils.text_utils import as_dir_name

SLIDES_PREFIX = "slides_"

SKIP_DIRS = frozenset(
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
        "pu",
        "drawio",
        ".cargo",
        ".idea",
        ".vscode",
        "target",
        "out",
        "CMakeFiles",
    )
)

PLANTUML_EXTENSIONS = {".pu", ".puml", ".plantuml"}

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


def is_ignored_dir(dir_path: Path) -> bool:
    for part in dir_path.parts:
        if part in SKIP_DIRS:
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
            self, "output_dir", self.root_dir / f"{lang}/{format_}/{mode}"
        )

    def __iter__(self):
        return iter((self.lang, self.format, self.mode, self.output_dir))


def output_specs(root_dir: Path):
    for lang_dir in [Lang.DE, Lang.EN]:
        for format_dir in [Format.HTML, Format.NOTEBOOK]:
            for mode_dir in [Mode.CODE_ALONG, Mode.COMPLETED]:
                yield OutputSpec(
                    lang=lang_dir,
                    format=format_dir,
                    mode=mode_dir,
                    root_dir=root_dir,
                )
    for lang_dir in [Lang.DE, Lang.EN]:
        yield OutputSpec(
            lang=lang_dir,
            format=Format.CODE,
            mode=Mode.COMPLETED,
            root_dir=root_dir,
        )
