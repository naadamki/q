class DatabaseError(Exception):
    """Base exception for database errors"""
    pass


class NotFoundError(DatabaseError):
    """Raised when a requested item is not found"""
    pass


class ValidationError(DatabaseError):
    """Raised when input validation fails"""
    pass


class DuplicateError(DatabaseError):
    """Raised when attempting to create a duplicate record"""
    pass


class Validator:
    """Helper class for validation with chainable error raising"""
    
    def __init__(self, condition: bool):
        self.condition = condition
    
    def raiseValidationError(self, message: str) -> None:
        """Raise ValidationError if condition is True"""
        if self.condition:
            raise ValidationError(message)

    def raiseNotImplementedError(self, message: str) -> None:
        """Raise NotImplementedError if condition is True"""
        if self.condition:
            raise NotImplementedError(message)        
    
    def raiseDuplicateError(self, message: str) -> None:
        """Raise DuplicateError if condition is True"""
        if self.condition:
            raise DuplicateError(message)
    
    def raiseNotFoundError(self, message: str) -> None:
        """Raise NotFoundError if condition is True"""
        if self.condition:
            raise NotFoundError(message)
    
    def raiseError(self, error_type, message: str) -> None:
        """Raise any error type if condition is True"""
        if self.condition:
            raise error_type(message)


def assert_that(condition: bool) -> Validator:
    """Create a validator for the given condition"""
    return Validator(condition)