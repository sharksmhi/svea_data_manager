import logging
import pathlib
import re
import datetime

from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions
from svea_data_manager.sdm_event import post_event

from ifcb.metadata import MetadataIFCB
from ifcb.hdr_file import HdrFile

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
        resource = IFCBResourceRaw.from_source_file(self.source_directory, source_file)
        if not resource:
            resource = IFCBResourceProcessed.from_source_file(self.source_directory, source_file)
        return resource

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def transform_package(self, package, **kwargs):
        # Look for hdr and metadata file
        metadata_file = None
        hdr_resource = None
        for resource in package.resources:
            if resource.source_path.suffix == '.txt':
                metadata_file = MetadataIFCB.from_file(resource.absolute_source_path)
            elif resource.source_path.suffix == '.hdr':
                hdr_resource = resource
        if not hdr_resource:
            return
        if not metadata_file:
            metadata_file = MetadataIFCB(id=hdr_resource.absolute_source_path.stem)

        # Get metadata from hdr file
        meta = HdrFile(hdr_resource.absolute_source_path).metadata

        # Add external metadata:
        ext_meta = kwargs.get('attributes', self.config.get('attributes', {}))
        if meta.get('quality_flag') == 'B':
            ext_meta.pop('quality_flag', None)
        meta.update(ext_meta)
        metadata_file.add(**meta)
        name = hdr_resource.absolute_source_path.stem + '.txt'
        reso = IFCBResourceRaw.from_string_content(metadata_file.get_string_content(), file_name=name, attributes=hdr_resource.attributes)
        package.resources.add(reso)
        post_event('on_transform_add_file', dict(instrument=self.name, resource=reso, name=name))

    def write_package(self, package):
        logger.info('Writing package %s to file storage' % package)
        return self._storage.write(package, self.config.get('force', False))


class IFCBResource(Resource):

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
               f"_" \
               f"{self.attributes['instrument']}"


class IFCBResourceRaw(IFCBResource):
    RAW_FILE_SUFFIXES = ['.adc', '.hdr', '.roi']

    PATTERNS = [
        re.compile('^D{}{}{}T{}{}{}_{}$'.format('(?P<year>\d{4})',
                                                '(?P<month>\d{2})',
                                                '(?P<day>\d{2})',
                                                '(?P<hour>\d{2})',
                                                '(?P<minute>\d{2})',
                                                '(?P<second>\d{2})',
                                                '(?P<instrument>IFCB\d*)',
                                                )
                   ),
    ]

    @property
    def target_path(self):
        subdir = f"D{self.attributes['year']}{self.attributes['month']}{self.attributes['day']}"
        file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        return pathlib.Path(self.attributes['instrument'], 'data_raw', f"D{self.attributes['year']}", subdir, file_name)

    @staticmethod
    def from_source_file(root_directory, source_file):
        if source_file.suffix.lower() not in IFCBResourceRaw.RAW_FILE_SUFFIXES:
            return
        for PATTERN in IFCBResourceRaw.PATTERNS:
            name_match = PATTERN.search(source_file.stem)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceRaw(root_directory, source_file, attributes)


class IFCBResourceProcessed(IFCBResource):

    PATTERNS = [
        re.compile('^D{}{}{}T{}{}{}_{}_{}{}.zip$'.format('(?P<year>\d{4})',
                                                            '(?P<month>\d{2})',
                                                            '(?P<day>\d{2})',
                                                            '(?P<hour>\d{2})',
                                                            '(?P<minute>\d{2})',
                                                            '(?P<second>\d{2})',
                                                            '(?P<instrument>IFCB\d*)',
                                                            '(?P<process_type>blobs)',
                                                            '(?P<version>.*)',
                                                            )
                   ),

        re.compile('^D{}{}{}T{}{}{}_{}_{}{}.csv$'.format('(?P<year>\d{4})',
                                                                '(?P<month>\d{2})',
                                                                '(?P<day>\d{2})',
                                                                '(?P<hour>\d{2})',
                                                                '(?P<minute>\d{2})',
                                                                '(?P<second>\d{2})',
                                                                '(?P<instrument>IFCB\d*)',
                                                                '(?P<process_type>fea)',
                                                                '(?P<version>.*)',
                                                                )
                   ),

        re.compile('^D{}{}{}T{}{}{}_{}_{}{}.csv$'.format('(?P<year>\d{4})',
                                                                '(?P<month>\d{2})',
                                                                '(?P<day>\d{2})',
                                                                '(?P<hour>\d{2})',
                                                                '(?P<minute>\d{2})',
                                                                '(?P<second>\d{2})',
                                                                '(?P<instrument>IFCB\d*)',
                                                                '(?P<process_type>multiblob)',
                                                                '(?P<version>.*)',
                                                                )
                   ),
    ]

    @property
    def target_path(self):
        process_type = self.attributes["process_type"]
        if process_type == 'fea':
            process_type = 'features'
        subdir = f"D{self.attributes['year']}{self.attributes['month']}{self.attributes['day']}"
        file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        return pathlib.Path(self.attributes['instrument'], f'{process_type}',
                            f"D{self.attributes['year']}", subdir, file_name)

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceProcessed.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceProcessed(root_directory, source_file, attributes)


class IFCBResourceClassification(IFCBResource):

    PATTERNS = [
        re.compile('^D{}{}{}T{}{}{}_{}_{}{}.mat$'.format('(?P<year>\d{4})',
                                                         '(?P<month>\d{2})',
                                                         '(?P<day>\d{2})',
                                                         '(?P<hour>\d{2})',
                                                         '(?P<minute>\d{2})',
                                                         '(?P<second>\d{2})',
                                                         '(?P<instrument>IFCB\d*)',
                                                         '(?P<process_type>class)',
                                                         '(?P<version>.*)',
                                                         )
                   ),

        re.compile('^summary_allTB_{}.mat$'.format('(?P<year>\d{4})')
                   ),
    ]

    @property
    def target_path(self):
        subdir = f"D{self.attributes['year']}{self.attributes['month']}{self.attributes['day']}"
        file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        return pathlib.Path(self.attributes['instrument'], f'{self.attributes["process_type"]}',
                            f"D{self.attributes['year']}", subdir, file_name)

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceClassification.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceClassification(root_directory, source_file, attributes)



