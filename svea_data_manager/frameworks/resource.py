from pathlib import Path

from svea_data_manager.frameworks import exceptions
from svea_data_manager.frameworks.helpers import verify_path


class Resource:

    def __init__(self, source_directory, path, attributes = {}):
        self._source_directory = Path(source_directory)

        path = verify_path(path)
        self._source_path = path
        self._target_path = path

        if type(attributes) is not dict:
            raise TypeError(
                'attributes must be dict, '
                'not {}.'.format(type(attributes))
            )

        self._attributes = attributes

    def __str__(self):
        return str(self.source_path)

    @property
    def attributes(self):
        return self._attributes

    @property
    def absolute_source_path(self):
        fullpath = self.source_directory.joinpath(self.source_path)
        return fullpath.resolve()

    @property
    def source_directory(self):
        return self._source_directory

    @property
    def source_path(self):
        return self._source_path

    @property
    def target_path(self):
        return self._target_path

    @target_path.setter
    def target_path(self, path):
        self._target_path = verify_path(path)


class ResourceCollection:

    def __init__(self):
        self._resources = {}
    
    def __iter__(self):
        return iter(self._resources.values())

    def add(self, resource):
        if not isinstance(resource, Resource):
            raise TypeError(
                'Only instances of Resource can be added to this '
                'collection, not {}.'.format(type(resource))
            )
        if self.has(resource):
            raise exceptions.ResourceAlreadyInCollection(
                'Resource {} could not be added to this '
                'collection since it already has been '
                'added.'.format(resource)
            )
        self._resources[str(resource)] = resource

    def has(self, resource):
        return str(resource) in self._resources.keys()

    def get(self, resource):
        if not self.has(resource):
            raise exceptions.ResourceNotInCollection(
                'Resource {} does not exist in this '
                'collection.'.format(resource)
            )
        return self._resources[str(resource)]


    NotInCollection = exceptions.ResourceNotInCollection
    AlreadyInCollection = exceptions.ResourceAlreadyInCollection
