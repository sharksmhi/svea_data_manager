import logging
import pathlib
import re

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)


class IFCB(Instrument):
    name = 'IFCB'
    desc = 'Imaging FlowCytobot (IFCB)'

    def __init__(self, config):
        super().__init__(config)

        if 'target_directory' not in self._config:
            msg = 'Missing required configuration target_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)
        self._storage = FileStorage(self._config['target_directory'])

    def prepare_resource(self, source_file):
        return IFCBResource.from_source_file(self.source_directory, source_file)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info('Writing package %s to file storrage' % package)
        return self._storage.write(package, True)


class IFCBResource(Resource):
    RAW_FILE_SUFFIXES = ['.adc', '.hdr', '.roi']

    PATTERNS = [
        re.compile('^D{}{}{}T{}{}{}_IFCB{}$'.format('(?P<year>\d{4})',
                                                    '(?P<month>\d{2})',
                                                    '(?P<day>\d{2})',
                                                    '(?P<hour>\d{2})',
                                                    '(?P<minute>\d{2})',
                                                    '(?P<second>\d{2})',
                                                    '(?P<instrument_number>\d*)',
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
    def package_key(self):
        return f"D" \
               f"{self.attributes['year']}" \
               f"{self.attributes['month']}" \
               f"{self.attributes['day']}" \
               f"T" \
               f"{self.attributes['hour']}" \
               f"{self.attributes['minute']}" \
               f"{self.attributes['second']}" \
               f"_IFCB" \
               f"{self.attributes['instrument_number']}"

    @property
    def target_path(self):
        path = pathlib.Path(self.attributes['year'])
        subdir = f"D{self.attributes['year']}{self.attributes['month']}{self.attributes['day']}"
        file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        return pathlib.Path(path, subdir, file_name)
    
    @staticmethod
    def from_source_file(root_directory, source_file):
        if source_file.suffix.lower() not in IFCBResource.RAW_FILE_SUFFIXES:
            return
        for PATTERN in IFCBResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                return IFCBResource(root_directory, source_file, attributes)
