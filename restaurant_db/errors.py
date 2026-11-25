"""Infrastructure errors; business rejections are ordinary command results."""


class DatabaseError(Exception):
    pass


class ValidationError(DatabaseError):
    pass


class AlreadyOpen(DatabaseError):
    pass


class OwnershipError(DatabaseError):
    pass


class IdempotencyConflict(DatabaseError):
    pass


class CorruptJournal(DatabaseError):
    pass


class StorageUnavailable(DatabaseError):
    """Commit outcome may be unknown. Reopen and retry with the same request ID."""
