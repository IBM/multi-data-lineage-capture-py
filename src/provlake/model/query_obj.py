import uuid

class QueryObj:

    def __init__(self, id: str, query_text: str, description: str=None, label: str=None,
                    header: list=None, mime_type: str=None, parameters: dict=None):
        self.id = id
        if not self.id:
            self.id = str(uuid.uuid4()).replace('-', '_')
        self.query_text = query_text
        self.description = description
        self.label = label
        self.header = header
        self.mime_type = mime_type
        self.parameters = parameters

    def as_dict(self) -> dict:
        return {
            "id": self.id,
            "query_text": self.query_text,
            "description": self.description,
            "label": self.label,
            "header": self.header,
            "mimeType": self.mime_type,
            "parameters": self.parameters
        }
