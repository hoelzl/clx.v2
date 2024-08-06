from attr import define


@define
class Text:
    de: str
    en: str

    def __getitem__(self, item):
        return getattr(self, item)


TEXT_MAPPINGS = {
    "code": Text(de="Python", en="Python"),
    "html": Text(de="Html", en="Html"),
    "notebook": Text(de="Notebooks", en="Notebooks"),
    "code_along": Text(de="Code-Along", en="Code-Along"),
    "completed": Text(de="Completed", en="Completed"),
    "speaker": Text(de="Speaker", en="Speaker"),
    "slides": Text(de="Folien", en="Slides"),
}


def as_dir_name(name, lang):
    return TEXT_MAPPINGS[name][lang]


_PARENS_TO_REPLACE = "{}[]"
_REPLACEMENT_PARENS = "()" * (len(_PARENS_TO_REPLACE) // 2)
_CHARS_TO_REPLACE = r"/\$#%&<>*=^€|"
_REPLACEMENT_CHARS = "_" * len(_CHARS_TO_REPLACE)
_CHARS_TO_DELETE = r""";!?"'`.:"""
_STRING_TRANSLATION_TABLE = str.maketrans(
    _PARENS_TO_REPLACE + _CHARS_TO_REPLACE,
    _REPLACEMENT_PARENS + _REPLACEMENT_CHARS,
    _CHARS_TO_DELETE,
)


def sanitize_file_name(text: str):
    sanitized_text = text.strip().translate(_STRING_TRANSLATION_TABLE)
    return sanitized_text
