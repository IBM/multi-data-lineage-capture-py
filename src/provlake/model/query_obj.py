import uuid
from typing import List


class QueryObj:

    def __init__(self, id: str, query_text: str, description: str=None, label: str = None, mime_type: str = None,
                 parameters: List[str] = None, columns: List[str] = None):
        self.id = id
        if not self.id:
            self.id = str(uuid.uuid4()).replace('-', '_')
        self.query_text = query_text
        self.description = description
        self.label = label
        self.mime_type = mime_type
        self.parameters = parameters
        self.columns = columns

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "query_text": self.query_text,
            "description": self.description,
            "label": self.label,
            "columns": self.columns,
            "mime_type": self.mime_type,
            "parameters": self.parameters
        }
