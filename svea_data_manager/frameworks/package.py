import logging

from svea_data_manager.frameworks import ResourceCollection
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)


class Package:

    def __init__(self, package_key):
        if type(package_key) is not str:
            msg = 'package_key must be of type string, not {}.'.format(type(package_key))
            logger.error(msg)
            raise TypeError(msg)
        if len(package_key) == 0:
            msg = 'package_key string must not be empty.'
            logger.error(msg)
            raise ValueError(msg)

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

    def __len__(self):
        return len(self._packages)

    def add(self, package):
        if not isinstance(package, Package):
            msg = 'Only instances of Package can be added to this collection, not {}.'.format(type(package))
            logger.error(msg)
            raise TypeError(msg)
        if self.has(package):
            msg = 'Package {} could not be added to this collection since it already has been added.'.format(package)
            logger.error(msg)
            raise exceptions.PackageAlreadyInCollection(msg)
        self._packages[str(package)] = package

    def has(self, package):
        return str(package) in self._packages.keys()

    def get(self, package):
        if not self.has(package):
            msg = 'Package {} does not exist in this collection.'.format(package)
            logger.debug(msg)
            raise exceptions.PackageNotInCollection(msg)
        return self._packages[str(package)]


    AlreadyInCollection = exceptions.PackageAlreadyInCollection
    NotInCollection = exceptions.PackageNotInCollection
