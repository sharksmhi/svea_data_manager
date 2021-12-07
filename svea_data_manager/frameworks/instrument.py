from pathlib import Path
import logging 

from svea_data_manager.frameworks import PackageCollection, Package
from svea_data_manager.frameworks import Resource
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)


class Instrument:
    name = None
    desc = None

    def __init__(self, config = {}):
        if not type(self.name) is str or len(self.desc) == 0:
            msg = 'Class property name must be defined as a non-empty string for {}'.format(self.__class__)
            logger.error(msg)
            raise TypeError(msg)

        if not type(self.desc) is str or len(self.desc) == 0:
            msg = 'Class property desc must be defined as a non-empty string for {}'.format(self.__class__)
            logger.error(msg)
            raise TypeError()

        if 'source_directory' not in config:
            msg = 'Missing required configuration source_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)

        self._config = config
        self._packages = None

    def __str__(self):
        return self.__class__.name

    def read_packages(self):
        self._packages = PackageCollection()

        for source_file in self.source_files:
            resource = self.prepare_resource(source_file)

            if not isinstance(resource, Resource):
                continue

            package_key = self.get_package_key_for_resource(resource)

            try:
                package = self.packages.get(package_key)
            except PackageCollection.NotInCollection:
                package = self.prepare_package(package_key)
                self.packages.add(package)
                logger.info(f'New package for added to PackageCollection: {package}')

            package.resources.add(resource)

    def transform_packages(self):
        for package in self.packages:
            self.transform_package(package)

    def write_packages(self):
        for package in self.packages:
            self.write_package(package)

    def get_package_key_for_resource(self, resource):
        return resource.source_path.stem

    def prepare_resource(self, source_file):
        return Resource(self.source_directory, source_file)

    def prepare_package(self, package_key):
        return Package(package_key)

    def transform_package(self, package):
        msg = 'Class {} has not implemented transform_package method.'.format(self.__class__.__name__)
        logger.error(msg)
        raise NotImplementedError(msg)

    def write_package(self, package):
        msg = 'Class {} has not implemented write_package method.'.format(self.__class__.__name__)
        logger.error(msg)
        raise NotImplementedError(msg)

    @property
    def packages(self):
        if not isinstance(self._packages, PackageCollection):
            msg = 'Packages has not yet been extracted for {}. Make sure to call the read_packages method before trying to access packages.'.format(self.__class__.__name__)
            logger.error(msg)
            raise exceptions.PackagesNotExtracted(msg)

        return self._packages

    @property
    def config(self):
        return self._config

    @property
    def source_directory(self):
        return Path(self.config['source_directory'])

    @property
    def source_files(self):
        found_files = self.source_directory.glob('**/*')
        return [
            file.relative_to(self.source_directory)
            for file in found_files if file.is_file()
        ]


    ImproperlyConfigured = exceptions.ImproperlyConfiguredInstrument
    PackagesNotExtracted = exceptions.PackagesNotExtracted
