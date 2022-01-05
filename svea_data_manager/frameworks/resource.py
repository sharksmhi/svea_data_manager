from pathlib import Path
import logging
import datetime

from svea_data_manager.frameworks import exceptions
from svea_data_manager.frameworks.helpers import check_path


logger = logging.getLogger(__name__)


class Resource:

    def __init__(self, source_directory, path, attributes = {}):
        self._source_directory = Path(source_directory)

        path = check_path(path)
        self._source_path = path
        self._target_path = path

        if type(attributes) is not dict:
            msg = 'attributes must be dict, not {}.'.format(type(attributes))
            logger.error(msg)
            raise TypeError(msg)

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
        self._target_path = check_path(path)

    @property
    def date(self):
        try:
            return datetime.datetime(int(self._attributes['year']),
                                     int(self._attributes['month']),
                                     int(self._attributes['day'])).date()
        except KeyError:
            return None

    @property
    def datetime(self):
        try:
            return datetime.datetime(int(self._attributes['year']),
                                     int(self._attributes['month']),
                                     int(self._attributes['day']),
                                     int(self._attributes['hour']),
                                     int(self._attributes['minute']),
                                     int(self._attributes['second']))
        except KeyError:
            return None


class ResourceCollection:

    def __init__(self):
        self._resources = {}
    
    def __iter__(self):
        return iter(self._resources.values())

    def __len__(self):
        return len(self._resources)

    def add(self, resource):
        if not isinstance(resource, Resource):
            msg = 'Only instances of Resource can be added to this collection, not {}.'.format(type(resource))
            logging.error(msg)
            raise TypeError(msg)
        if self.has(resource):
            msg = 'Resource {} could not be added to this collection since it already has been added.'.format(resource)
            logging.error(msg)
            raise exceptions.ResourceAlreadyInCollection(msg)
        self._resources[str(resource)] = resource

    def has(self, resource):
        return str(resource) in self._resources.keys()

    def get(self, resource):
        if not self.has(resource):
            msg = 'Resource {} does not exist in this collection.'.format(resource)
            logging.error(msg)
            raise exceptions.ResourceNotInCollection(msg)
        return self._resources[str(resource)]


    NotInCollection = exceptions.ResourceNotInCollection
    AlreadyInCollection = exceptions.ResourceAlreadyInCollection
