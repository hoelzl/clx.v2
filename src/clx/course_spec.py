import io
from pathlib import Path
from xml.etree import ElementTree as ETree

from attr import Factory, frozen
from clx.utils.text_utils import Text, as_dir_name


@frozen
class TopicSpec:
    id: str


@frozen
class SectionSpec:
    name: Text
    topics: list[TopicSpec] = Factory(list)


@frozen
class CourseSpec:
    name: Text
    prog_lang: str
    description: Text
    certificate: Text
    sections: list["SectionSpec"]
    github_repo: Text

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
        )


def parse_multilang(root: ETree.ElementTree, tag: str) -> Text:
    return Text(**{element.tag: element.text for element in root.find(tag)})
