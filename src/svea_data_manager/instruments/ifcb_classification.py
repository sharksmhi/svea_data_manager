import logging
import os
import pathlib
import re
import datetime
import shutil

from svea_data_manager.ifcb import mat_file
from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import exceptions
from svea_data_manager.sdm_event import post_event
from svea_data_manager import helpers

logger = logging.getLogger(__name__)


SUB_DIRECTORIES = ['manual', 'blobs', 'features']


def get_key_from_path(path: pathlib.Path) -> str:
    return '_'.join(path.stem.split('_')[:2])


class IFCBclassification(Instrument):
    name = 'IFCBclassification'
    desc = 'Classification of Imaging FlowCytobot (IFCB)'

    def __init__(self, config):
        super().__init__(config)

        self._source_root_directory: pathlib.Path | None = None
        self._source_root_sub_directories: dict[str, pathlib.Path] = {}

        self._sub_directories_paths: dict = {}
        self._paths_by_classifier: dict = {}

        self._set_source_root_directory()
        self._set_sub_directories()
        self._find_sub_directories_paths()

        if 'target_directory' not in self._config:
            msg = 'Missing required configuration target_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)

        self._storage = FileStorage(self._config['target_directory'])

    def _set_source_root_directory(self):
        if not self._try_set_given_source_root_directory():
            self._try_set_source_root_directory_from_source_directory()

    def _try_set_given_source_root_directory(self):
        # Sets if given
        root_dir = self._config.get('source_root_directory')  # This is the root directory used to find all files assosiated with the classification
        if root_dir:
            self._source_root_directory = pathlib.Path(root_dir)
            if not self._source_root_directory.exists():
                raise NotADirectoryError(f'Given source root directory does not exist: {self._source_root_directory}')
            return True

    def _try_set_source_root_directory_from_source_directory(self):
        post_event('log', 'Trying to set source_root_directory')
        root_dir_parts = []
        for part in pathlib.Path(self.source_directory).parts:
            if part == 'classified':
                break
            root_dir_parts.append(part)
        else:
            return
        self._source_root_directory = pathlib.Path(*root_dir_parts)
        return True

    def _set_sub_directories(self):
        """Check that all necessary root subdirectories are present"""
        if not self._source_root_directory:
            return
        for sub in SUB_DIRECTORIES:
            path = self._source_root_directory / sub
            if not path.exists():
                raise NotADirectoryError(f'Missing {sub}-directory: {path}')
            self._source_root_sub_directories[sub] = path

    def _find_sub_directories_paths(self):
        for sub in ['manual', 'blobs', 'features']:
            self._sub_directories_paths[sub] = {}
            post_event('log', f'Finding paths in {sub}')
            for root, dirs, files in os.walk(self._source_root_sub_directories[sub], topdown=False):
                for name in files:
                    path = pathlib.Path(root, name)
                    if not path.is_file():
                        continue
                    self._sub_directories_paths[sub][get_key_from_path(path)] = path

    def prepare_resource(self, source_file: pathlib.Path):
        for cls in [
            # IFCBResourceResult,
            IFCBResourceClassification,
            # IFCBResourceManual,
            # IFCBResourceConfig,
            # IFCBResourceSummary,

        ]:
            source_directory = self.source_directory
            if helpers.get_temp_directory() in source_file.parents:
                source_directory = helpers.get_temp_directory()
                source_file = source_file.relative_to(helpers.get_temp_directory())
            resource = cls.from_source_file(source_directory, source_file)
            if resource:
                return resource

        # resource = IFCBResourceRaw.from_source_file(self.source_directory, source_file)
        # if not resource:
        #     resource = IFCBResourceProcessed.from_source_file(self.source_directory, source_file)
        # return resource

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def read_packages(self):
        super().read_packages()
        for pack in self.packages:
            for resource in pack.resources:
                self._paths_by_classifier.setdefault(resource.package_key, {})
                for sub in SUB_DIRECTORIES:
                    self._paths_by_classifier[resource.package_key].setdefault(sub, {})
                    # print(f'{sub}: {get_key_from_path(resource.absolute_source_path)=}')
                    key = get_key_from_path(resource.absolute_source_path)
                    path = self._sub_directories_paths[sub].get(key)
                    if not path:
                        # post_event('log', f'No matching {sub}-file matching {resource.stem}')
                        # print(f'{sub}   {resource.stem=}')
                        continue
                    self._paths_by_classifier[resource.package_key][sub][key] = path

    # def transform_package(self, package, **kwargs):
    #     clas = set()
    #     for resource in package.resources:
    #         clas.add(resource.classifier_name)
    #     print(f'{clas=}')

    def write_packages(self):
        for pack in self.packages:
            for resource in pack.resources:
                instrument = resource.attributes['instrument']
                break
            target_directory = pathlib.Path(self._config['target_directory']) / instrument / 'results' / pack.package_key
            if target_directory.exists():
                post_event('log', f'Target path exists: {target_directory}. Will not save!')
                raise IsADirectoryError(target_directory)
            target_directory.mkdir(parents=True)

            # classified
            class_directory = target_directory / 'classified'
            class_directory.mkdir()
            for resource in pack.resources:
                target_path = class_directory / resource.absolute_source_path.name
                shutil.copy2(resource.absolute_source_path, target_path)

            # subdirs
            for sub in SUB_DIRECTORIES:
                target_sub_directory = target_directory / sub
                target_sub_directory.mkdir()
                if sub == 'manual':
                    for source_path in self._sub_directories_paths[sub].values():
                        target_path = target_sub_directory / source_path.name
                        shutil.copy2(source_path, target_path)
                else:
                    for source_path in self._paths_by_classifier[pack.package_key][sub].values():
                        target_path = target_sub_directory / source_path.name
                        shutil.copy2(source_path, target_path)


class IFCBResourceClassification(Resource):

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

        re.compile('^summary_allTB_{}.mat$'.format('(?P<year>\d{4})')),
        re.compile('^summary_biovol_allTB2{}.mat$'.format('(?P<year>\d{4})')),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mat_file_obj = mat_file.ClassifierMatFile(self.absolute_source_path)

    @property
    def date_str(self):
        return self.attributes['year'] + self.attributes['month'] + self.attributes['day']

    @property
    def time_str(self):
        return self.attributes['hour'] + self.attributes['minute']

    @property
    def stem(self):
        return self.absolute_source_path.stem

    @property
    def package_key(self):
        return f'{self.absolute_source_path.parent.name}_{self.mat_file_obj.classifier_name}'

        # if self.attributes.get('minute'):
        #     return f"D" \
        #            f"{self.attributes['year']}" \
        #            f"{self.attributes['month']}" \
        #            f"{self.attributes['day']}" \
        #            f"T" \
        #            f"{self.attributes['hour']}" \
        #            f"{self.attributes['minute']}" \
        #            f"{self.attributes['second']}" \
        #            f"_" \
        #            f"{self.attributes['instrument']}"
        # elif self.source_path.suffix == '.csv':
        #     return f"summary"
        # elif self.source_path.suffix == '.mat':
        #     return f"config"

    @property
    def target_path(self):
        return
        # subdir = f"D{self.attributes['year']}{self.attributes['month']}{self.attributes['day']}"
        # file_name = f'{self.source_path.stem.upper()}{self.source_path.suffix.lower()}'
        # return pathlib.Path(self.attributes['instrument'], f'{self.attributes["process_type"]}',
        #                     f"D{self.attributes['year']}", subdir, file_name)

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceClassification.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceClassification(root_directory, source_file, attributes)

    @property
    def classifier_name(self):
        return self.mat_file_obj.classifier_name


# class IFCBResourceManual(IFCBResource):
#
#     PATTERNS = [
#         re.compile('^D{}{}{}T{}{}{}_{}.mat$'.format('(?P<year>\d{4})',
#                                                          '(?P<month>\d{2})',
#                                                          '(?P<day>\d{2})',
#                                                          '(?P<hour>\d{2})',
#                                                          '(?P<minute>\d{2})',
#                                                          '(?P<second>\d{2})',
#                                                          '(?P<instrument>IFCB\d*)',
#                                                          )
#                    ),
#     ]
#
#     @property
#     def target_path(self):
#         return
#
#     @staticmethod
#     def from_source_file(root_directory, source_file):
#         for PATTERN in IFCBResourceManual.PATTERNS:
#             name_match = PATTERN.search(source_file.name)
#             if name_match:
#                 attributes = name_match.groupdict()
#                 return IFCBResourceManual(root_directory, source_file, attributes)
#
#
# class IFCBResourceSummary(IFCBResource):
#
#     PATTERNS = [
#         re.compile('^biovolume.csv$'),
#         re.compile('^class2use.csv$'),
#         re.compile('^classcount.csv$'),
#         re.compile('^date.csv$'),
#         re.compile('^filelistTB.csv$'),
#         re.compile('^ml_analyzed.csv$'),
#         # re.compile('^results_{}{}{}T{}{}{}_{}.mat$'.format('(?P<day>\d{2})',
#         #                                                  '(?P<month>\D+)',
#         #                                                  '(?P<year>\d{4})',
#         #                                                  '(?P<hour>\d{2})',
#         #                                                  '(?P<minute>\d{2})',
#         #                                                  '(?P<second>\d{2})',
#         #                                                  '(?P<instrument>IFCB\d*)',
#         #                                                  )
#         #            ),
#     ]
#
#     @property
#     def target_path(self):
#         return
#
#     @staticmethod
#     def from_source_file(root_directory, source_file):
#         for PATTERN in IFCBResourceSummary.PATTERNS:
#             name_match = PATTERN.search(source_file.name)
#             if name_match:
#                 attributes = name_match.groupdict()
#                 return IFCBResourceSummary(root_directory, source_file, attributes)
#
#
# class IFCBResourceConfig(IFCBResource):
#
#     PATTERNS = [
#         re.compile('^class2use_{}.mat$'.format('(?P<area>.+)')),
#         re.compile('^(?P<area>.+)[.]mcconfig.mat$'),
#     ]
#
#     @property
#     def target_path(self):
#         return
#
#     @staticmethod
#     def from_source_file(root_directory, source_file):
#         for PATTERN in IFCBResourceConfig.PATTERNS:
#             name_match = PATTERN.search(source_file.name)
#             if name_match:
#                 attributes = name_match.groupdict()
#                 return IFCBResourceConfig(root_directory, source_file, attributes)
#
#
# class IFCBResourceResult(IFCBResource):
#
#     PATTERNS = [
#         re.compile('^result_{}_{}{}{}_{}{}{}{}$'.format('(?P<instrument>IFCB\d*)',
#                                                          '(?P<year>\d{4})',
#                                                          '(?P<month>\d{2})',
#                                                          '(?P<day>\d{2})',
#                                                          '(?P<hour>\d{2})',
#                                                          '(?P<minute>\d{2})',
#                                                          '(?P<second>\d{2})',
#                                                          '(?P<suffix>\.zip|\.txt)',
#                                                          )
#                    ),
#
#     ]
#
#     @property
#     def target_path(self):
#         return pathlib.Path(self.attributes['instrument'], f'results', self.source_path.name)
#
#     @staticmethod
#     def from_source_file(root_directory, source_file):
#         for PATTERN in IFCBResourceResult.PATTERNS:
#             name_match = PATTERN.search(source_file.name)
#             if name_match:
#                 attributes = name_match.groupdict()
#                 print(f'{root_directory=}')
#                 print(f'{source_file=}')
#                 return IFCBResourceResult(root_directory, source_file, attributes)



