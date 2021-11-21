from svea_data_manager.frameworks import ResourceCollection
from svea_data_manager.frameworks import exceptions


class Package:

    def __init__(self, package_key):
        if type(package_key) is not str:
            raise TypeError(
                'package_key must be of type string, '
                'not {}.'.format(type(package_key))
            )
        if len(package_key) == 0:
            raise ValueError('package_key string must not be empty.')

        self._package_key = package_key
        self._resources = ResourceCollection()

    def __str__(self):
        return self._package_key

    @property
    def resources(self):
        return self._resources


class PackageCollection:

    def __init__(self):
        self._packages = {}
    
    def __iter__(self):
        return iter(self._packages.values())

    def add(self, package):
        if not isinstance(package, Package):
            raise TypeError(
                'Only instances of Package can be added to this '
                'collection, not {}.'.format(type(package))
            )
        if self.has(package):
            raise exceptions.PackageAlreadyInCollection(
                'Package {} could not be added to this '
                'collection since it already has been '
                'added.'.format(package)
            )
        self._packages[str(package)] = package

    def has(self, package):
        return str(package) in self._packages.keys()

    def get(self, package):
        if not self.has(package):
            raise exceptions.PackageNotInCollection(
                'Package {} does not exist in this '
                'collection.'.format(package)
            )
        return self._packages[str(package)]


    AlreadyInCollection = exceptions.PackageAlreadyInCollection
    NotInCollection = exceptions.PackageNotInCollection
