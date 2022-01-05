import logging
import pathlib
import re
import shutil

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)


class ADCP(Instrument):
    name = 'ADCP'
    desc = 'ADCP monitoring from Svea'

    def __init__(self, config):
        super().__init__(config)
        self._storage = FileStorage(self._config['target_directory'])

    def prepare_resource(self, source_file):
        resource = ADCPResourceRaw.from_source_file(self.source_directory, source_file)
        if not resource:
            resource = ADCPResourceProcessed.from_source_file(self.source_directory, source_file)
        return resource

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info('Writing package %s to subversion repo' % package)
        return self._storage.write(package, self._config.get('force', False))


# class ADCPOS150(ADCP):
#     name = 'ADCPOS150'
#     desc = 'ADCPOS150 monitoring from Svea'
#     source_subdir = 'ADCPOS150'
#     target_subdir = 'ADCPOS150'
#
#
# class ADCPWHM600(ADCP):
#     name = 'ADCPWHM600'
#     desc = 'ADCPWHM600 monitoring from Svea'
#     source_subdir = 'ADCPWHM600'
#     target_subdir = 'ADCPWHM600'

class ADCPResource(Resource):
    @property
    def package_key(self):
        return f"{self.attributes['instrument']}_{self.attributes['ship']}_{self.attributes['year']}_{self.attributes['cruise']}"


class ADCPResourceRaw(ADCPResource):
    PATTERNS = [
        # ADCPOS150_77SE_2022_01_000_00000
        re.compile('^{}_{}_{}_{}_{}_{}$'.format('(?P<instrument>ADCP\w+)',
                                                '(?P<ship>\d{2}\D{2})',
                                                '(?P<year>\d{4})',
                                                '(?P<cruise>\d{2})',
                                                '(?P<counter>.+)',
                                                '(?P<nr>.+)')),
        re.compile('^{}_{}_{}_{}_{}$'.format('(?P<instrument>ADCP\w+)',
                                                '(?P<ship>\d{2}\D{2})',
                                                '(?P<year>\d{4})',
                                                '(?P<cruise>\d{2})',
                                                '(?P<nr>.+)'))

    ]
    @property
    def target_path(self):
        parts_list = [self.attributes['instrument'], self.attributes['year'], self.package_key, 'raw', self.source_path.name]
        # Assuring instrument sub folder in instrument class
        return pathlib.Path(*parts_list)


    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in ADCPResourceRaw.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                attributes['suffix'] = source_file.suffix
                resource = ADCPResourceRaw(root_directory, source_file, attributes)
                return resource


class ADCPResourceProcessed(ADCPResource):
    VALID_PATH_IDS = {'OS_LTA': 'ADCPOS150',
                      'WH_LTA': 'ADCPWH600'}

    PATTERNS = [
        re.compile('{}_{}_{}_utdata'.format('(?P<ship>\d{2}\D{2})',
                                            '(?P<year>\d{4})',
                                            '(?P<cruise>\d{2})'))
        ]

    @property
    def target_path(self):
        root = pathlib.Path(self.attributes['instrument'], self.attributes['year'], self.package_key, 'processed', self.attributes['processed_type'])
        if self.attributes['suffix'] == '.nc':
            path = pathlib.Path(root, f'{self.package_key}{self.attributes["suffix"]}')
        else:
            file_name = f'{self.package_key}_{self.source_path.name}'
            if self.attributes['suffix'] == '.png':
                path = pathlib.Path(root, 'plots', file_name)
            else:
                path = pathlib.Path(root, 'info', file_name)
        return path

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in ADCPResourceProcessed.PATTERNS:
            name_match = PATTERN.search(str(pathlib.Path(root_directory, source_file)))

            if name_match:
                attributes = name_match.groupdict()
                attributes['suffix'] = source_file.suffix
                for key, value in ADCPResourceProcessed.VALID_PATH_IDS.items():
                    if key in str(pathlib.Path(root_directory, source_file)):
                        attributes['processed_type'] = key
                        attributes['instrument'] = value
                        break

                if not attributes.get('instrument'):
                    return

                resource = ADCPResourceProcessed(root_directory, source_file, attributes)
                return resource



