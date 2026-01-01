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

