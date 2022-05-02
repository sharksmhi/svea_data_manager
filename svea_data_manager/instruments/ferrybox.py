import logging
import pathlib
import re

from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import Instrument, Resource, Package
from svea_data_manager.frameworks import SubversionStorage
from svea_data_manager.frameworks import exceptions

logger = logging.getLogger(__name__)


class FERRYBOX(Instrument):
    name = 'Ferrybox'
    desc = 'Ferrybox monitoring from Svea'

    def __init__(self, config):
        super().__init__(config)
        if 'subversion_repo_url' not in self._config:
            msg = 'Missing required configuration subversion_repo_url.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)
        if 'target_directory' not in self._config:
            msg = 'Missing required configuration target_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)
        self._svn_storage = SubversionStorage(self._config['subversion_repo_url'])
        self._wiski_file_storage = FileStorage(self._config['target_directory'])  # Wiski

    def prepare_resource(self, source_file):
        return FerryboxResource.from_source_file(self.source_directory, source_file)

    def prepare_package(self, package_key):
        if 'wiski' in package_key.lower():
            return FerryboxPackageWiski(package_key)
        else:
            return FerryboxPackageSVN(package_key)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info('Writing package %s to subversion repo' % package)
        if isinstance(package, FerryboxPackageSVN):
            return self._svn_storage.write(package, self._config.get('force', False))
        elif isinstance(package, FerryboxPackageWiski):
            return self._wiski_file_storage.write(package, self._config.get('force', False))


class FerryboxPackageSVN(Package):
    pass


class FerryboxPackageWiski(Package):
    pass


class FerryboxResource(Resource):

    PATTERNS = [
        re.compile('^{}_{}-{}-{}{}$'.format('(?P<prefix>.+)',  # All_sensors (root)
                                                '(?P<year>\d{4})',
                                                '(?P<month>\d{2})',
                                                '(?P<day>\d{2})',
                                                '(?P<suffix>\D*)?')),
        re.compile('^{}_{}-{}-{}_{}-{}$'.format('(?P<prefix>.+)',  # All_sensors (toFTP)
                                                '(?P<year>\d{4})',
                                                '(?P<month>\d{2})',
                                                '(?P<day>\d{2})',
                                                '(?P<hour>\d{2})',
                                                '(?P<minute>\d{2})')),
        re.compile('^{} {}{}{} {}{}{}$'.format('(?P<prefix>.+)',  # CO2FT
                                                '(?P<year>\d{4})',
                                                '(?P<month>\d{2})',
                                                '(?P<day>\d{2})',
                                                '(?P<hour>\d{2})',
                                                '(?P<minute>\d{2})',
                                                '(?P<second>\d{2})')),
        re.compile('^{}_{}{}{}$'.format('(?P<prefix>.+)',  # GPS etc.
                                        '(?P<year>\d{4})',
                                        '(?P<month>\d{2})',
                                        '(?P<day>\d{2})'))
    ]

    @property
    def package_key(self):
        key = f"{self.attributes['year']}-{self.attributes['month']}-{self.attributes['day']}"
        if 'wiski' in self.source_path.stem:
            key = f'{key}-wiski'
        return key

    @property
    def target_path(self):
        if self.source_path.parent.name == 'Working':
            if self.attributes['prefix'].startswith('All_sensors'):
                parts_list = [self.attributes['year'], 'AllSensors', self.source_path.name]
                return pathlib.Path(*parts_list)
            elif self.attributes['prefix'].startswith('Watersampler'):
                parts_list = [self.attributes['year'], 'Watersampler', self.source_path.name]
                return pathlib.Path(*parts_list)
        else:
            index = 0
            parts = list(self.source_path.parts)
            if 'Working' in parts:
                index = parts.index('Working') + 1
            new_parts = parts[index:]
            parts_list = [self.attributes['year']] + new_parts
            return pathlib.Path(*parts_list)
    
    @staticmethod
    def from_source_file(root_directory, source_file):
        if 'FERRYBOX' not in str(pathlib.Path(root_directory, source_file)).upper():
            return None
        for PATTERN in FerryboxResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                resource = FerryboxResource(root_directory, source_file, attributes)
                # TODO: Move to validate
                if str(resource.date) == 'logger.error(msg)':
                    logger.warning(f'Not handling file found with date 1904-01-01: {resource.absolute_source_path}')
                    return None
                return resource
