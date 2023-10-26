class ProtocolError(Exception):
    def __init__(self, message: Exception):
        super().__init__(message)
