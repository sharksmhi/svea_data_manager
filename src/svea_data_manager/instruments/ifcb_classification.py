import datetime
import logging
import pathlib
import re

from svea_data_manager import helpers
from svea_data_manager.frameworks import FileStorage
from svea_data_manager.frameworks import Instrument, Resource
from svea_data_manager.frameworks import Package
from svea_data_manager.frameworks import exceptions
from svea_data_manager.ifcb import mat_file

logger = logging.getLogger(__name__)


def get_key_from_path(path: pathlib.Path) -> str:
    return '_'.join(path.stem.split('_')[:2])


class IFCBclassification(Instrument):
    name = 'IFCBclassification'
    desc = 'Classification of Imaging FlowCytobot (IFCB)'

    def __init__(self, config):
        super().__init__(config)

        self._source_root_directory: pathlib.Path | None = None

        if 'target_directory' not in self._config:
            msg = 'Missing required configuration target_directory.'
            logger.error(msg)
            raise exceptions.ImproperlyConfiguredInstrument(msg)

        self._storage = FileStorage(self._config['target_directory'])

    def prepare_resource(self, source_file: pathlib.Path):
        for cls in [
            IFCBResourceClass,
        ]:
            source_directory = self.source_directory
            if helpers.get_temp_directory() in source_file.parents:
                source_directory = helpers.get_temp_directory()
                source_file = source_file.relative_to(helpers.get_temp_directory())
            resource = cls.from_source_file(source_directory, source_file)
            if resource:
                return resource

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def transform_package(self, package, **kwargs) -> None:
        resource = self._get_class_resource_from_package(package)
        self._add_classifier_file(package, resource)
        self._add_manual_files(package, resource)
        self._add_config_files(package, resource)
        self._add_readme(package, resource)

    @staticmethod
    def _get_class_resource_from_package(package: Package) -> "IFCBResourceClass":
        resource = None
        for res in package.resources:
            if isinstance(res, IFCBResourceClass):
                resource = res
                break
        if not resource:
            raise exceptions.ResourceNotInCollection('No class file found')
        return resource

    @staticmethod
    def _add_classifier_file(package: Package, resource: "IFCBResourceClass") -> None:
        classifier_path = resource.classifier_path
        classifier = IFCBResourceClassifier(classifier_path.parent, pathlib.Path(classifier_path.name),
                                                class_file=resource)
        package.resources.add(classifier)

    @staticmethod
    def _add_manual_files(package: Package, resource: "IFCBResourceClass") -> None:
        for path in resource.manual_directory.iterdir():
            if path.is_dir():
                continue
            manual = IFCBResourceManual(path.parent, pathlib.Path(path.name), class_file=resource)
            package.resources.add(manual)

    @staticmethod
    def _add_config_files(package: Package, resource: "IFCBResourceClass") -> None:
        source_dir = resource.config_directory
        files = [
            source_dir / f'class2use_{resource.area_name}.mat',
            source_dir / f'config_{resource.area_name}.mcconfig.mat',
        ]
        for path in files:
            print(f'{path=}')
            if not path.exists():
                raise FileNotFoundError(path)
            config = IFCBResourceConfig(path.parent, pathlib.Path(path.name), class_file=resource)
            package.resources.add(config)

    def _add_readme(self, package: Package, resource: "IFCBResourceClass"):
        lines = [
            f'Package name: {resource.package_key}',
            f'Source directory: {resource.absolute_source_path.parent}',
            f'Classifier name: {resource.classifier_name}',
            f'Import datetime: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            f'Imported by: {pathlib.Path.home().name}',
        ]
        readme_path = helpers.get_temp_directory() / 'readme.txt'
        with open(readme_path, 'w') as fid:
            fid.write('\n'.join(lines))
        config = IFCBResourceClassReadme(readme_path.parent, pathlib.Path(readme_path.name), class_file=resource)
        package.resources.add(config)

    def write_package(self, package):
        resource = self._get_class_resource_from_package(package)
        package_path = pathlib.Path(self._config['target_directory']) / resource.package_path
        if package_path.exists():
            raise exceptions.TargetPathExists(package_path)
        logger.info('Writing package %s to file storage' % package)
        return self._storage.write(package)


class IFCBResourceClass(Resource):

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

        # re.compile('^summary_allTB_{}.mat$'.format('(?P<year>\d{4})')),
        # re.compile('^summary_biovol_allTB2{}.mat$'.format('(?P<year>\d{4})')),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._mat_file_obj: mat_file.ClassifierMatFile | None = None

    @property
    def mat_file_obj(self) -> mat_file.ClassifierMatFile:
        if not self._mat_file_obj:
            self._mat_file_obj = mat_file.ClassifierMatFile(self.absolute_source_path)
        return self._mat_file_obj

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
    def area_name(self) -> str:
        return self.absolute_source_path.parent.parent.name

    @property
    def classifier_name(self) -> str:
        return self.mat_file_obj.classifier_name

    @property
    def package_path(self) -> pathlib.Path:
        return pathlib.Path(self.attributes['instrument'], 'classifications', self.package_key)

    @property
    def classifier_path(self) -> pathlib.Path:
        base = str(self.absolute_source_path).split('classified')[0].strip(r'\\')
        rel_path = str(self.mat_file_obj.classifier_original_path).split('manual')[-1].strip(r'\\')
        path = pathlib.Path(base) / 'manual' / rel_path
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    @property
    def manual_directory(self) -> pathlib.Path:
        print(str(self.absolute_source_path).split('classified')[0])
        base = str(self.absolute_source_path).split('classified')[0].strip(r'\\')
        return pathlib.Path(base) / 'manual' / self.area_name

    @property
    def config_directory(self) -> pathlib.Path:
        base = str(self.absolute_source_path).split('classified')[0].strip(r'\\')
        return pathlib.Path(base) / 'config'

    @property
    def package_key(self) -> str:
        return f'{self.absolute_source_path.parent.name}@{self.classifier_name.split(".")[0]}'

    @property
    def target_path(self):
        return pathlib.Path(self.package_path, 'classified', self.source_path.name)

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceClass.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceClass(root_directory, source_file, attributes)


class IFCBResourceClassifier(Resource):

    def __init__(self, source_directory, path, attributes=None, class_file: IFCBResourceClass = None):
        attributes = attributes or {}
        super().__init__(source_directory, path, attributes)
        self._class_file = class_file

    @property
    def target_path(self):
        return pathlib.Path(self._class_file.attributes['instrument'], 'classifications',
                            self._class_file.package_key, 'manual', self._class_file.area_name, 'summary', self.source_path.name)


class IFCBResourceManual(Resource):

    def __init__(self, source_directory, path, attributes=None, class_file: IFCBResourceClass = None):
        attributes = attributes or {}
        super().__init__(source_directory, path, attributes)
        self._class_file = class_file

    @property
    def target_path(self):
        return pathlib.Path(self._class_file.attributes['instrument'], 'classifications',
                            self._class_file.package_key, 'manual', self._class_file.area_name, self.source_path.name)


class IFCBResourceConfig(Resource):

    def __init__(self, source_directory, path, attributes=None, class_file: IFCBResourceClass = None):
        attributes = attributes or {}
        super().__init__(source_directory, path, attributes)
        self._class_file = class_file

    @property
    def target_path(self):
        return pathlib.Path(self._class_file.attributes['instrument'], 'classifications',
                            self._class_file.package_key, 'config', self.source_path.name)


class IFCBResourceClassReadme(Resource):

    def __init__(self, source_directory, path, attributes=None, class_file: IFCBResourceClass = None):
        attributes = attributes or {}
        super().__init__(source_directory, path, attributes)
        self._class_file = class_file

    @property
    def target_path(self):
        return pathlib.Path(self._class_file.attributes['instrument'], 'classifications',
                            self._class_file.package_key, 'readme.txt')



