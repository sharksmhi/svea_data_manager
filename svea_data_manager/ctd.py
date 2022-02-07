import logging
import pathlib
import re

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import SubversionStorage
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)

SHIPS = {'77_10': '77SE'}


class CTD(Instrument):
    name = 'CTD'
    desc = 'Conductivity, temperature and depth monitoring from Svea'

    def __init__(self, config):
        super().__init__(config)

        if 'subversion_repo_url' not in self._config:
            raise exceptions.ImproperlyConfiguredInstrument(
                'Missing required configuration subversion_repo_url.'
            )
        self._storage = SubversionStorage(self._config['subversion_repo_url'])
        # self._storage = FileStorage(self._config['target_directory'])

    def prepare_resource(self, source_file):
        return CTDResource.from_source_file(self.source_directory, source_file)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info('Writing package %s to subversion repo' % package)
        return self._storage.write(package, True)


class CTDResource(Resource):
    RAW_FILE_SUFFIXES = ['.bl', '.btl', '.hdr', '.hex', '.ros', '.xmlcon', '.xml']

    PATTERNS = [
        re.compile('^{}{}_{}_{}{}{}_{}{}_{}_{}$'.format('(?P<prefix>u|d)?',
                                                        '(?P<instrument>SBE\d{2})',
                                                        '(?P<instrument_number>\d{4})',
                                                        '(?P<year>\d{4})',
                                                        '(?P<month>\d{2})',
                                                        '(?P<day>\d{2})',
                                                        '(?P<hour>\d{2})',
                                                        '(?P<minute>\d{2})',
                                                        '(?P<ship>\d{2}_\w{2})',
                                                        '(?P<serno>\d{4})',
                                                        )
                   ),
        re.compile('^{}{}_{}_{}{}{}_{}{}_{}_{}_{}$'.format('(?P<prefix>u)?',
                                                        '(?P<instrument>SBE\d{2})',
                                                        '(?P<instrument_number>\d{4})',
                                                        '(?P<year>\d{4})',
                                                        '(?P<month>\d{2})',
                                                        '(?P<day>\d{2})',
                                                        '(?P<hour>\d{2})',
                                                        '(?P<minute>\d{2})',
                                                        '(?P<ship>\d{2}\w{2})',
                                                        '(?P<cruise>\d{2})',
                                                        '(?P<serno>\d{4})',
                                                        )
                   ),
        ]

    @property
    def date_str(self):
        return self.attributes['year'] + self.attributes['month'] + self.attributes['day']

    @property
    def time_str(self):
        return self.attributes['hour'] + self.attributes['minute']

    @property
    def ship(self):
        return SHIPS.get(self.attributes['ship'], self.attributes['ship'])

    @property
    def cruise(self):
        return self.attributes.get('cruise', '00')

    @property
    def package_key(self):
        return f"{self.attributes['instrument']}_{self.attributes['instrument_number']}_{self.date_str}_{self.time_str}_" \
               f"{self.ship}_{self.cruise}_{self.attributes['serno']}"

    @property
    def target_path(self):
        path = pathlib.Path(self.attributes['year'])

        if self.source_path.suffix == '.cnv':
            if self.attributes.get('prefix'):
                if self.attributes.get('prefix', '').lower() == 'u':
                    path = pathlib.Path(path, 'cnv', 'upcast')
                elif self.attributes.get('prefix', '').lower() == 'd':
                    path = pathlib.Path(path, 'cnv', 'downcast')
            else:
                path = pathlib.Path(path, 'cnv')
        elif self.source_path.suffix.lower() in CTDResource.RAW_FILE_SUFFIXES:
            path = pathlib.Path(path, 'raw')
        elif self.source_path.suffix == '.txt':
            pass
        file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        if self.attributes.get('suffix'):
            file_name = self.attributes['suffix'].lower() + file_name[1:]
        return path.joinpath(file_name)
    
    @staticmethod
    def from_source_file(root_directory, source_file):
        if source_file.suffix.lower() not in CTDResource.RAW_FILE_SUFFIXES + ['.cnv', '.txt']:
            return
        for PATTERN in CTDResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                return CTDResource(root_directory, source_file, attributes)
