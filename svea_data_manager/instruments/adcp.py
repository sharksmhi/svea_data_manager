import logging
import pathlib
import re
import shutil
import datetime

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions


CRUISE_NOT_NEEDED_AFTER_DATE = datetime.date(2023, 1, 1)

CRUISE_MAPPING = {
    (datetime.date(2022, 1, 10), datetime.date(2022, 1, 13)): '1',  # January
    (datetime.date(2022, 2, 8), datetime.date(2022, 2, 10)): '3',  # February
    (datetime.date(2022, 3, 17), datetime.date(2022, 3, 21)): '5',  # Mars
    (datetime.date(2022, 4, 20), datetime.date(2022, 4, 25)): '7',  # April
    (datetime.date(2022, 5, 19), datetime.date(2022, 5, 26)): '10',  # May
    (datetime.date(2022, 6, 13), datetime.date(2022, 6, 17)): '11',  # June
    (datetime.date(2022, 7, 12), datetime.date(2022, 7, 17)): '12',  # Juli
    (datetime.date(2022, 8, 12), datetime.date(2022, 8, 19)): '13',  # August
}


logger = logging.getLogger(__name__)


def get_start_date_from_log_file(path):
    with open(path) as fid:
        for line in fid:
            date_strings = re.findall(r'\d{4}/\d{2}/\d{2}', line)
            if not date_strings:
                continue
            return datetime.datetime.strptime(date_strings[0], '%Y/%m/%d').date()


def get_start_date_from_cruise_info_file(path):
    with open(path) as fid:
        for line in fid:
            if line.startswith('CRUISE DATES'):
                date_strings = re.findall(r'\d{4}/\d{2}/\d{2}', line)
                if not date_strings:
                    raise ValueError
                return datetime.datetime.strptime(date_strings[0], '%Y/%m/%d').date()


def get_cruise_number_for_date(date):
    if date > CRUISE_NOT_NEEDED_AFTER_DATE:
        return True
    for key, value in CRUISE_MAPPING.items():
        fr, to = key
        if fr <= date <= to:
            return value
    return False


class ADCP(Instrument):
    name = 'ADCP'
    desc = 'ADCP monitoring from Svea'

    def __init__(self, config):
        super().__init__(config)
        self._storage = FileStorage(self._config['target_directory'])
        self._package_key_attributes = {}

    def prepare_resource(self, source_file):
        resource = ADCPResourceRaw.from_source_file(self.source_directory, source_file)
        if not resource:
            resource = ADCPResourceProcessed.from_source_file(self.source_directory, source_file)
        if not resource:
            resource = ADCPResourceReadme.from_source_file(self.source_directory, source_file)
        return resource

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def transform_packages(self, **kwargs):
        self._package_key_attributes = {}
        for package in self.packages:
            attributes = self.transform_package(package, **kwargs)
            if str(package) == 'readme':
                continue
            self._package_key_attributes[str(package)] = attributes

    def transform_package(self, package, **kwargs):
        date = None
        attributes = kwargs.get('attributes', {})
        resource = {}
        for resource in package.resources:
            if not date:
                if resource.source_path.stem == 'cruise_info':
                    date = get_start_date_from_cruise_info_file(resource.absolute_source_path)
                elif resource.source_path.suffix.upper() == '.LOG':
                    date = get_start_date_from_log_file(resource.absolute_source_path)
                if date:
                    cruise = get_cruise_number_for_date(date)
                    if cruise is True:
                        break
                    elif cruise is False:
                        if attributes.get('cruise'):
                            msg = f"Cruise for package {str(package)} if set from the outside to {attributes.get('cruise')}"
                            logger.warning(msg)
                        else:
                            msg = f"No internal mapping for cruise was found for package {str(package)}"
                            logger.error(msg)
                            raise exceptions.CruiseError(msg)
                    else:
                        msg = f"Cruise for package {str(package)} if set by internal mapping from {attributes.get('cruise')} to {cruise}"
                        logger.warning(msg)
                        attributes['cruise'] = cruise
                    break

        for resource in package.resources:
            resource.attributes.update(attributes)
            package._package_key = resource.package_key
        return resource.attributes.copy()

    def write_package(self, package):
        logger.info('Writing package %s to subversion repo' % package)
        # return self._storage.write(package, self._config.get('force', False))
        if str(package) == 'readme':
            for key, attr in self._package_key_attributes.items():
                attr['package_key'] = key
                for resource in package.resources:
                    resource.attributes.update(attr)
                self._storage.write(package, self._config.get('force', False))
        else:
            return self._storage.write(package, self._config.get('force', False))


class ADCPResource(Resource):
    INSTRUMENT_MAPPING = {
        'ADCPWHM600': 'ADCPWH600'
    }

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
                attributes['instrument'] = ADCPResourceProcessed.INSTRUMENT_MAPPING.get(attributes['instrument'], attributes['instrument'])
                resource = ADCPResourceRaw(root_directory, source_file, attributes)
                return resource
        logger.info(f'Not patterns match for file: {source_file}')


class ADCPResourceProcessed(ADCPResource):
    VALID_PATH_IDS = {
        'OS_LTA': 'ADCPOS150',
        'WH_LTA': 'ADCPWH600',
        }

    PATTERNS = [
        # re.compile('{}_{}_{}_utdata'.format('(?P<ship>\d{2}\D{2})',
        #                                     '(?P<year>\d{4})',
        #                                     '(?P<cruise>\d{2})')),
        re.compile('{}_{}_{}_{}_utdata'.format('(?P<instrument>ADCP\w+)',
                                             '(?P<ship>\d{2}\D{2})',
                                             '(?P<year>\d{4})',
                                             '(?P<cruise>\d{2})'))
        ]

    @property
    def target_path(self):
        root = pathlib.Path(self.attributes['instrument'], self.attributes['year'], self.package_key, 'processed')
        # root = pathlib.Path(self.attributes['instrument'], self.attributes['year'], self.package_key, 'processed', self.attributes['processed_type'])
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
                        attributes['instrument'] = value
                        break
                attributes['instrument'] = ADCPResourceProcessed.INSTRUMENT_MAPPING.get(attributes['instrument'], attributes['instrument'])

                # if not attributes.get('instrument'):
                #     return

                resource = ADCPResourceProcessed(root_directory, source_file, attributes)
                return resource


class ADCPResourceReadme(ADCPResource):
    @property
    def package_key(self):
        return 'readme'

    @property
    def target_path(self):
        parts_list = [self.attributes['instrument'], self.attributes['year'], self.attributes['package_key'], self.source_path.name.lower()]
        return pathlib.Path(*parts_list)

    @staticmethod
    def from_source_file(root_directory, source_file):
        if 'readme' in str(source_file):
            resource = ADCPResourceReadme(root_directory, source_file)
            return resource
        logger.info(f'Not patterns match for file: {source_file}')



