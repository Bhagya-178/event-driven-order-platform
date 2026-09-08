class ApplicationError(Exception):
    """Base class for all application errors."""
    def __init__(self, message: str, code: str = "INTERNAL_ERROR"):
        self.message = message
        self.code = code
        super().__init__(self.message)

class NotFoundError(ApplicationError):
    def __init__(self, message: str):
        super().__init__(message, code="NOT_FOUND")

class ValidationError(ApplicationError):
    def __init__(self, message: str):
        super().__init__(message, code="VALIDATION_ERROR")

class ConflictError(ApplicationError):
    def __init__(self, message: str):
        super().__init__(message, code="CONFLICT")
