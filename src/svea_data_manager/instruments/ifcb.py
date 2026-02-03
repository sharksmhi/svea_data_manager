import datetime
import logging
import pathlib
import re

from svea_data_manager import helpers
from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import FileStorage
from svea_data_manager.ifcb.hdr_file import HdrFile
from svea_data_manager.ifcb.metadata import MetadataIFCB
from svea_data_manager.instruments import exceptions
from svea_data_manager.sdm_event import post_event

logger = logging.getLogger(__name__)

YEAR = r"(?P<year>\d{4})"
MONTH = r"(?P<month>\d{2})"
DAY = r"(?P<day>\d{2})"
HOUR = r"(?P<hour>\d{2})"
MINUTE = r"(?P<minute>\d{2})"
SECOND = r"(?P<second>\d{2})"
INSTRUMENT = r"(?P<instrument>IFCB\d*)"
PROCESS_BLOBS = r"(?P<process_type>blobs)"
PROCESS_FEATURES = r"(?P<process_type>fea)"
PROCESS_MULTIBLOB = r"(?P<process_type>multiblob)"
VERSION = r"(?P<version>.*)"


class IFCB(Instrument):
    name = "IFCB"
    desc = "Imaging FlowCytobot (IFCB)"

    def __init__(self, config):
        super().__init__(config)

        if "target_directory" not in self._config:
            msg = "Missing required configuration target_directory."
            logger.error(msg)
            raise exceptions.InstrumentConfigurationError(msg)
        self._storage = FileStorage(self._config["target_directory"])

    def prepare_resource(self, source_file: pathlib.Path):
        for cls in [
            IFCBResourceRaw,
            IFCBResourceBlobs,
            IFCBResourceFeatures,
            IFCBResourceMultiBlob,
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

    def transform_packages(self):
        super().transform_packages()
        # self._create_result_package()

    def transform_package(self, package, **kwargs):
        # Look for hdr and metadata file
        metadata_file = None
        hdr_resource = None
        for resource in package.resources:
            if resource.source_path.suffix == ".txt":
                metadata_file = MetadataIFCB.from_file(resource.absolute_source_path)
            elif resource.source_path.suffix == ".hdr":
                hdr_resource = resource
        if not hdr_resource:
            return
        if not metadata_file:
            metadata_file = MetadataIFCB(id=hdr_resource.absolute_source_path.stem)

        # Get metadata from hdr file
        meta = HdrFile(hdr_resource.absolute_source_path).metadata

        # Add external metadata:
        ext_meta = kwargs.get("attributes", self.config.get("attributes", {}))
        if meta.get("quality_flag") == "B":
            ext_meta.pop("quality_flag", None)
        meta.update(ext_meta)
        metadata_file.add(**meta)
        name = hdr_resource.absolute_source_path.stem + ".txt"
        reso = IFCBResourceRaw.from_string_content(
            metadata_file.get_string_content(),
            file_name=name,
            attributes=hdr_resource.attributes,
        )
        package.resources.add(reso)
        post_event(
            "on_transform_add_file",
            dict(instrument=self.name, resource=reso, name=name),
        )

    def write_package(self, package):
        logger.info("Writing package %s to file storage" % package)
        return self._storage.write(package, self.config.get("force", False))

    @staticmethod
    def _get_result_file_stem(instrument):
        return f"result_{instrument}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def _create_result_txt_file(self, file_paths, txt_result_file_path):
        with open(txt_result_file_path, "w") as fid:
            fid.write("\n".join([str(path) for path in file_paths]))
        reso = self.add_file(txt_result_file_path)
        post_event(
            "on_transform_add_file",
            dict(instrument=self.name, resource=reso, name=txt_result_file_path),
        )


class IFCBResource(Resource):
    @property
    def date_str(self):
        return self.attributes["year"] + self.attributes["month"] + self.attributes["day"]

    @property
    def time_str(self):
        return self.attributes["hour"] + self.attributes["minute"]

    @property
    def package_key(self):
        if self.attributes.get("minute"):
            return (
                f"D"
                f"{self.attributes['year']}"
                f"{self.attributes['month']}"
                f"{self.attributes['day']}"
                f"T"
                f"{self.attributes['hour']}"
                f"{self.attributes['minute']}"
                f"{self.attributes['second']}"
                f"_"
                f"{self.attributes['instrument']}"
            )
        elif self.source_path.suffix == ".csv":
            return "summary"
        elif self.source_path.suffix == ".mat":
            return "config"


class IFCBResourceRaw(IFCBResource):
    RAW_FILE_SUFFIXES = (".adc", ".hdr", ".roi")

    PATTERNS = (
        re.compile(
            rf"^D{YEAR}{MONTH}{DAY}"
            rf"T{HOUR}{MINUTE}{SECOND}"
            rf"_{INSTRUMENT}$"
        ),
    )

    @property
    def target_path(self):
        subdir = (
            f"D{self.attributes['year']}"
            f"{self.attributes['month']}"
            f"{self.attributes['day']}"
        )
        file_name = f"{self.source_path.stem}{self.source_path.suffix.lower()}"
        return pathlib.Path(
            self.attributes["instrument"],
            "data",
            self.attributes["year"],
            subdir,
            file_name,
        )

    @staticmethod
    def from_source_file(root_directory, source_file):
        if source_file.suffix.lower() not in IFCBResourceRaw.RAW_FILE_SUFFIXES:
            return
        for PATTERN in IFCBResourceRaw.PATTERNS:
            name_match = PATTERN.search(source_file.stem)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceRaw(root_directory, source_file, attributes)


class IFCBResourceBlobs(IFCBResource):
    PATTERNS = (
        re.compile(
            rf"^D{YEAR}{MONTH}{DAY}"
            rf"T{HOUR}{MINUTE}{SECOND}"
            rf"_{INSTRUMENT}_{PROCESS_BLOBS}{VERSION}.zip$"
        ),
    )

    @property
    def target_path(self):
        subdir = (
            f"D{self.attributes['year']}"
            f"{self.attributes['month']}"
            f"{self.attributes['day']}"
        )
        file_name = f"{self.source_path.stem}{self.source_path.suffix.lower()}"
        return pathlib.Path(
            self.attributes["instrument"],
            self.attributes["process_type"],
            self.attributes["year"],
            subdir,
            file_name,
        )

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceBlobs.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceBlobs(root_directory, source_file, attributes)


class IFCBResourceFeatures(IFCBResource):
    PATTERNS = (
        re.compile(
            rf"^D{YEAR}{MONTH}{DAY}"
            rf"T{HOUR}{MINUTE}{SECOND}"
            rf"_{INSTRUMENT}_{PROCESS_FEATURES}{VERSION}.csv$"
        ),
    )

    @property
    def target_path(self):
        process_type = self.attributes["process_type"]
        if process_type == "fea":
            process_type = "features"
        file_name = f"{self.source_path.stem}{self.source_path.suffix.lower()}"
        return pathlib.Path(
            self.attributes["instrument"],
            process_type,
            self.attributes["year"],
            file_name,
        )

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceFeatures.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceFeatures(root_directory, source_file, attributes)


class IFCBResourceMultiBlob(IFCBResource):
    PATTERNS = (
        re.compile(
            rf"^D{YEAR}{MONTH}{DAY}"
            rf"T{HOUR}{MINUTE}{SECOND}"
            rf"_{INSTRUMENT}_{PROCESS_MULTIBLOB}{VERSION}.csv$"
        ),
    )

    @property
    def target_path(self):
        file_name = f"{self.source_path.stem}{self.source_path.suffix.lower()}"
        return pathlib.Path(
            self.attributes["instrument"],
            "features",
            self.attributes["year"],
            self.attributes["process_type"],
            file_name,
        )

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in IFCBResourceMultiBlob.PATTERNS:
            name_match = PATTERN.search(source_file.name)
            if name_match:
                attributes = name_match.groupdict()
                return IFCBResourceMultiBlob(root_directory, source_file, attributes)
