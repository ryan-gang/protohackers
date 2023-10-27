class ProtocolError(Exception):
    def __init__(self, message: str | Exception):
        super().__init__(message)
