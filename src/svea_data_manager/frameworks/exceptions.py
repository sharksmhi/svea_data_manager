from svea_data_manager.exceptions import SveaDataManagerError


class FrameworksError(SveaDataManagerError):
    """Base class for exceptions in frameworks module."""

    pass


class PackagesNotExtractedError(FrameworksError):
    """The instrument packages has not been extracted."""

    pass


class PackageAlreadyInCollectionError(FrameworksError):
    """The package already exist in the package collection"""

    pass


class PackageNotInCollectionError(FrameworksError):
    """The package does not exist in the package collection"""

    pass


class ResourceAlreadyInCollectionError(FrameworksError):
    """The resource already exist in the resource collection"""

    pass


class PackageKeyNotFoundForResourceError(FrameworksError):
    """No package key found for resource"""

    pass


class ResourceAlreadyInStorageError(FrameworksError):
    """The resource already exists in the storage"""

    pass


class ForceNotAllowedError(FrameworksError):
    """Not allowed to force"""

    pass


class UnknownPackageTypeError(FrameworksError):
    """The package type is unknown"""

    pass


class ConfigurationError(FrameworksError):
    """Something is missing in the configuration"""

    pass


class StorageError(FrameworksError):
    pass


class StorageRootDirectoryDoesNotExistError(StorageError):
    """The storage root directory does not exist"""

    pass


class MissingSubversionExecutableError(StorageError):
    """Missing Subversion executable needed for storage"""

    pass


class SubversionError(StorageError):
    """A Subversion related error"""

    pass
