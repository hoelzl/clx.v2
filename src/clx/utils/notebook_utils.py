import re

from clx.utils.text_utils import Text, sanitize_file_name

TITLE_REGEX = re.compile(
    r"{{\s*header\s*\(\s*[\"'](.*)[\"']\s*,\s*[\"'](.*)[\"']\s*\)\s*}}"
)


def find_notebook_titles(text: str, default: str | None) -> Text:
    """Find the titles from the source text of a notebook."""
    match = TITLE_REGEX.search(text)
    if match:
        return Text(de=sanitize_file_name(match[1]), en=sanitize_file_name(match[2]))
    if default:
        return Text(de=default, en=default)
    raise ValueError("No title found.")


IMG_REGEX = re.compile(r'<img\s+src="([^"]+)"')


def find_images(text: str) -> frozenset[str]:
    return frozenset(IMG_REGEX.findall(text))


IMPORT_REGEX = re.compile(r"(?:from\s+(\S+)\s+import|import\s+(\S+))", re.MULTILINE)


def find_imports(text: str) -> frozenset[str]:
    return frozenset(match.group(1) or match.group(2) for match in
                     IMPORT_REGEX.finditer(text))


