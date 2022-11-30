from pathlib import Path
import logging
import datetime

from svea_data_manager.frameworks import PackageCollection, Package
from svea_data_manager.frameworks import Resource
from svea_data_manager.frameworks import exceptions

logger = logging.getLogger(__name__)


class InstrumentLogger:
    def __init__(self, name):
        self.name = name
        self._info = []
        self._accepted = set()
        self._declined = set()
        self._time = datetime.datetime.now()
        self.start()

    @property
    def accepted_resources(self):
        return self._accepted

    @property
    def declined_resources(self):
        return self._declined

    def start(self):
        self._info.append(f'Start at: {self._time}')

    def stop(self):
        time = datetime.datetime.now()
        self._info.append(f'Stop at: {time}')
        self._info.append(f'Finished in: {str(time - self._time)}')
        self._info.append(f'Nr accepted files: {len(self._accepted)}')
        self._info.append(f'Nr declined files: {len(self._declined)}')

    def add_accepted(self, info):
        self._accepted.add(str(info))

    def add_declined(self, info):
        self._declined.add(str(info))

    def get_report(self) -> dict:
        """Returns a dictionary with report information"""
        data = dict(
            info=self._info,
            accepted=self._accepted,
            declined=self._declined
        )
        return data

    def get_report_text(self) -> str:
        """Returns a report as string"""
        lines = [self.name]
        lines.extend(self._info)
        lines.append('')
        lines.append('Declined files')
        lines.extend(self._declined)
        lines.append('')
        lines.append('Accepted files')
        lines.extend(self._accepted)
        return '\n'.join(lines)

    def write_report(self, directory):
        date_str = self._time.strftime('%Y%m%d%H%M')
        self._write_accepted(Path(directory, f'{self.name}_{date_str}_accepted.txt'))
        self._write_declined(Path(directory, f'{self.name}_{date_str}_declined.txt'))
        self._write_info(Path(directory, f'{self.name}_{date_str}_info.txt'))
        return

    def _write_info(self, path):
        with open(path, 'w') as fid:
            fid.write('\n'.join(self._info))

    def _write_accepted(self, path):
        with open(path, 'w') as fid:
            fid.write('\n'.join(sorted(self._accepted)))

    def _write_declined(self, path):
        with open(path, 'w') as fid:
            fid.write('\n'.join(sorted(self._declined)))


class Instrument:
    name = None
    desc = None

    def __init__(self, config={}):
        if not type(self.name) is str or len(self.desc) == 0:
            msg = 'Class property name must be defined as a non-empty string for {}'.format(self.__class__)
            logger.error(msg)
            raise TypeError(msg)

        if not type(self.desc) is str or len(self.desc) == 0:
            msg = 'Class property desc must be defined as a non-empty string for {}'.format(self.__class__)
            logger.error(msg)
            raise TypeError(msg)

        if 'source_directory' not in config:
            msg = 'Missing required configuration source_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)

        self._instrument_logger = InstrumentLogger(self.name)
        self._config = config
        self._packages = None

    def __str__(self):
        return self.__class__.name

    def read_packages(self):
        self._packages = PackageCollection()

        for source_file in self.source_files:
            resource = self.prepare_resource(source_file)

            if not isinstance(resource, Resource):
                self._instrument_logger.add_declined(source_file)
                logger.warning(
                    "Don't know how to handle source file: %s. "
                    "Skipping file." % source_file
                )
                continue

            package_key = self.get_package_key_for_resource(resource)

            try:
                package = self.packages.get(package_key)
            except PackageCollection.NotInCollection:
                package = self.prepare_package(package_key)
                self.packages.add(package)
                logger.info(f'New package for added to PackageCollection: {package}')

            package.resources.add(resource)
            self._instrument_logger.add_accepted(source_file)

    def transform_packages(self, **kwargs):
        for package in self.packages:
            self.transform_package(package, **kwargs)

    def write_packages(self):
        for package in self.packages:
            self.write_package(package)
        self._instrument_logger.stop()

    def get_package_key_for_resource(self, resource):
        return resource.source_path.stem

    def prepare_resource(self, source_file):
        return Resource(self.source_directory, source_file)

    def prepare_package(self, package_key):
        return Package(package_key)

    def transform_package(self, package, **kwargs):
        return
        msg = f'Class {self.__class__.__name__} has not implemented transform_package method.'
        logger.error(msg)
        raise NotImplementedError(msg)

    def write_package(self, package):
        msg = f'Class {self.__class__.__name__} has not implemented write_package method.'
        logger.error(msg)
        raise NotImplementedError(msg)

    @property
    def packages(self):
        if not isinstance(self._packages, PackageCollection):
            msg = f'Packages has not yet been extracted for {self.__class__.__name__}. ' \
                  f'Make sure to call the read_packages method before trying to access packages.'
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

    def get_report(self):
        return self._instrument_logger.get_report()

    def get_report_text(self):
        return self._instrument_logger.get_report_text()

    def write_report(self, directory):
        return self._instrument_logger.write_report(directory)

    ImproperlyConfigured = exceptions.ImproperlyConfiguredInstrument
    PackagesNotExtracted = exceptions.PackagesNotExtracted
