import logging
import pathlib
import re

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import SubversionStorage
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions


logger = logging.getLogger(__name__)


class MVP(Instrument):
    name = 'MVP'
    desc = 'MVP monitoring from Svea'
    source_subdir = 'MVP'
    target_subdir = 'MVP'

    def __init__(self, config):
        super().__init__(config)
        if 'subversion_repo_url' not in self._config:
            raise exceptions.ImproperlyConfiguredInstrument(
                'Missing required configuration subversion_repo_url.'
            )
        self._storage = SubversionStorage(self._config['subversion_repo_url'])
        # self._storage = FileStorage(self._config['target_directory'])

    def prepare_resource(self, source_file):
        return MVPResource.from_source_file(self.source_directory, source_file)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info('Writing package %s to subversion repo' % package)
        return self._storage.write(package, self._config.get('force', False))


class MVPResource(Resource):

    PATTERNS = [
        re.compile('^{}{}_{}-{}-{}_{}{}{}_{}$'.format('(?P<prefix>.{1})?',
                                                      '(?P<instrument>MVP)',  # PROCESSED
                                                      '(?P<year>\d{4})',
                                                      '(?P<month>\d{2})',
                                                      '(?P<day>\d{2})',
                                                      '(?P<hour>\d{2})',
                                                      '(?P<minute>\d{2})',
                                                      '(?P<second>\d{2})',
                                                      '(?P<cut>.+-.+)'), re.I),
        re.compile('^{}{}_{}-{}-{}_{}{}{}{}$'.format('(?P<prefix>.{1})?',
                                                     '(?P<instrument>MVP)',  # RAWDATA
                                                     '(?P<year>\d{4})',
                                                     '(?P<month>\d{2})',
                                                     '(?P<day>\d{2})',
                                                     '(?P<hour>\d{2})',
                                                     '(?P<minute>\d{2})',
                                                     '(?P<second>\d{2})',
                                                     '(?P<ending>_.*)?'))

    ]

    @property
    def package_key(self):
        return f"{self.attributes['year']}-{self.attributes['month']}-{self.attributes['day']} " \
               f"{self.attributes['hour']}:{self.attributes['minute']}:{self.attributes['second']}"

    @property
    def target_path(self):
        parts = list(self.source_path.parts)
        cut = 'unknown'
        for i, part in enumerate(parts):
            if part.startswith('SMHI_'):
                cut = parts[i + 1]
                break
        if 'RAWDATA' in parts:
            parts_list = [self.attributes['year'], cut, 'raw', self.source_path.name]
            return pathlib.Path(*parts_list)
        if self.source_path.suffix == '.cnv':
            if self.attributes['prefix'] == 'u':
                parts_list = [self.attributes['year'], cut, 'cnv', 'upcast', self.source_path.name]
            elif self.attributes['prefix'] == 'd':
                parts_list = [self.attributes['year'], cut, 'cnv', 'downcast', self.source_path.name]
            else:
                parts_list = [self.attributes['year'], cut, 'cnv', self.source_path.name]
            return pathlib.Path(*parts_list)
        if self.source_path.suffix == '.jpg':
            parts_list = [self.attributes['year'], cut, 'cnv', 'downcast', 'plot', self.source_path.name]
            return pathlib.Path(*parts_list)
        return pathlib.Path('annat', self.source_path.name)  # Temporary while testing
    
    @staticmethod
    def from_source_file(root_directory, source_file):
        path_str = str(pathlib.Path(root_directory, source_file)).upper()
        if 'MVP' not in path_str:
            return None
        if 'SMHI_' not in path_str:
            return None
        for PATTERN in MVPResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                if not attributes.get('cut') and 'RAWDATA' in source_file.parts:
                    attributes['cut'] = source_file.parent.name
                attributes['cut'] = attributes['cut'].upper()
                attributes['suffix'] = source_file.suffix
                resource = MVPResource(root_directory, source_file, attributes)
                return resource
