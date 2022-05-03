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
        ret = {
            "id": self.id,
            "query_text": self.query_text,
            "mime_type": self.mime_type,
        }
        if self.label:
            ret["label"] = self.label
        if self.description:
            ret["description"] = self.description
        if self.columns:
            ret["columns"] = self.columns
        if self.parameters:
            ret["parameters"] = self.parameters
        return ret
