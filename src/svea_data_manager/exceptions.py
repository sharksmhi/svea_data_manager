class SveaDataManagerError(Exception):
    pass


class ResourceNotInCollection(SveaDataManagerError):
    """The resource does not exist in the resource collection"""

    pass
