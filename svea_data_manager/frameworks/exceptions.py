class ImproperlyConfiguredInstrument(Exception):
    """The instrument is missing something in it's configuration"""
    pass

class PackagesNotExtracted(Exception):
    """The instrument packages has not been extracted."""
    pass

class PackageAlreadyInCollection(Exception):
    """The package already exist in the package collection"""
    pass

class PackageNotInCollection(Exception):
    """The package does not exist in the package collection"""
    pass

class ResourceAlreadyInCollection(Exception):
    """The resource already exist in the resource collection"""
    pass

class ResourceNotInCollection(Exception):
    """The resource does not exist in the resource collection"""
    pass

class PackageKeyNotFoundForResource(Exception):
    """No package key found for resource"""
    pass
