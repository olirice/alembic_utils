class ReplaceableObject:
    """A SQL Entity that can be replaced"""

    def __init__(self, schema: str, signature: str, definition: str):
        self.schema = schema
        self.signature = signature
        self.definition = definition
