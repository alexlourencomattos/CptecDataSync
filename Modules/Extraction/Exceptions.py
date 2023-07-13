class EmptyDatabaseException(Exception):
    """
    Throws when try to retrieve last sync and database is empty
    """
    pass


class ExtractionException(Exception):
    pass
