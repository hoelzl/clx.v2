from pydantic import BaseModel


class NotebookPayload(BaseModel):
    notebook_text: str
    notebook_path: str
    reply_stream: str
    prog_lang: str
    language: str
    notebook_format: str
    output_type: str
    other_files: dict[str, str]
