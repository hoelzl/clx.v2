import io
import logging
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ETree

from attr import Factory, frozen, field
from clx.utils.text_utils import Text, as_dir_name

logger = logging.getLogger(__name__)


@frozen
class TopicSpec:
    id: str


@frozen
class SectionSpec:
    name: Text
    topics: list[TopicSpec] = Factory(list)


@frozen
class DictGroupSpec:
    name: Text
    path: str
    subdirs: list[str] | None = None
    include_top_level_files: bool = True

    @classmethod
    def from_element(cls, element: ETree.Element):
        subdirs = [
            subdir_element.text for subdir_element in element.find("subdirs") or []
        ]
        include_top_level_files = element.get("include-top-level-files")
        if include_top_level_files is not None:
            include_top_level_files = include_top_level_files.lower() == "true"
        else:
            include_top_level_files = True
        name = Text.from_string(element.find("name").text or "")
        return cls(
            name=name,
            path=element.find("path").text,
            subdirs=subdirs,
            include_top_level_files=include_top_level_files,
        )


@frozen
class CourseSpec:
    name: Text
    prog_lang: str
    description: Text
    certificate: Text
    sections: list[SectionSpec]
    github_repo: Text
    dictionaries: list[DictGroupSpec] = field(factory=list)

    @property
    def topics(self) -> list[TopicSpec]:
        return [topic for section in self.sections for topic in section.topics]

    @staticmethod
    def parse_sections(root) -> list[SectionSpec]:
        sections = []
        for i, section_elem in enumerate(root.findall("sections/section"), start=1):
            name = parse_multilang(root, f"sections/section[{i}]/name")
            topics = [
                TopicSpec(id=topic_elem.text)
                for topic_elem in section_elem.find("topics").findall("topic")
            ]
            sections.append(SectionSpec(name=name, topics=topics))
        return sections

    @staticmethod
    def parse_dict_groups(root) -> list[DictGroupSpec]:
        dict_groups = []
        for dict_group in root.findall("dict-groups/dict-group"):
            dict_groups.append(DictGroupSpec.from_element(dict_group))
        return dict_groups

    @classmethod
    def from_file(cls, xml_file: Path | io.IOBase) -> "CourseSpec":
        tree = ETree.parse(xml_file)
        root = tree.getroot()

        return cls(
            name=parse_multilang(root, "name"),
            prog_lang=root.find("prog-lang").text,
            description=parse_multilang(root, "description"),
            certificate=parse_multilang(root, "certificate"),
            github_repo=parse_multilang(root, "github"),
            sections=cls.parse_sections(root),
            dictionaries=cls.parse_dict_groups(root),
        )


def parse_multilang(root: ETree.ElementTree, tag: str) -> Text:
    return Text(**{element.tag: element.text for element in root.find(tag)})
