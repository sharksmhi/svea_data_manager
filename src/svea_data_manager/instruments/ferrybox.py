import datetime
import logging
import pathlib
import re

from svea_data_manager.frameworks.exceptions import UnknownPackageTypeError
from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.frameworks.package import Package
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import FileStorage
from svea_data_manager.instruments import exceptions

logger = logging.getLogger(__name__)

PREFIX = r"(?P<prefix>.+)"
YEAR = r"(?P<year>\d{4})"
MONTH = r"(?P<month>\d{2})"
DAY = r"(?P<day>\d{2})"
HOUR = r"(?P<hour>\d{2})"
MINUTE = r"(?P<minute>\d{2})"
SECOND = r"(?P<second>\d{2})"
SUFFIX = r"(?P<suffix>\D*)?"
FROM_YEAR = r"(?P<from_year>\d{4})"
FROM_MONTH = r"(?P<from_month>\d{2})"
FROM_DAY = r"(?P<from_day>\d{2})"
TO_YEAR = r"(?P<to_year>\d{4})"
TO_MONTH = r"(?P<to_month>\d{2})"
TO_DAY = r"(?P<to_day>\d{2})"
NAME = r"(?P<name>.+)"


class Ferrybox(Instrument):
    name = "Ferrybox"
    desc = "Ferrybox monitoring from Svea"

    def __init__(self, config):
        super().__init__(config)
        if "target_directory" not in self._config:
            msg = "Missing required configuration target_directory."
            logger.error(msg)
            raise exceptions.InstrumentConfigurationError(msg)
        # if 'wiski_directory' not in self._config:
        #     msg = 'Missing required configuration wiski_directory.'
        #     logger.error(msg)
        #     raise exceptions.ImproperlyConfiguredInstrument(msg)
        self._file_storage = FileStorage(self._config["target_directory"])
        # self._wiski_storage = FileStorage(self._config['wiski_directory'])  # Wiski

    def prepare_resource(self, source_file):
        resource = FerryboxResourceRaw.from_source_file(
            self.source_directory, source_file
        )
        if not resource:
            resource = FerryboxResourceCO2.from_source_file(
                self.source_directory, source_file
            )
        if not resource:
            resource = FerryboxResourceWiski.from_source_file(
                self.source_directory, source_file
            )
        return resource

    def prepare_package(self, package_key):
        if "wiski" in package_key.lower():
            return FerryboxPackageWiski(package_key, instrument=self.name)
        else:
            return FerryboxPackageStorage(package_key, instrument=self.name)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package: Package):
        if isinstance(package, FerryboxPackageStorage):
            logger.info("Writing package %s to subversion repo" % package)
            return self._file_storage.write(package, self._config.get("force", False))
        elif isinstance(package, FerryboxPackageWiski):
            logger.info("Writing package %s to wiski file storage repo" % package)
            return self._wiski_storage.write(package, self._config.get("force", False))
        raise UnknownPackageTypeError(f"Unknown package type: {type(package)}")


class FerryboxPackageStorage(Package):
    pass


class FerryboxPackageWiski(Package):
    pass


class FerryboxResourceRaw(Resource):
    PATTERNS = (
        re.compile(rf"^{PREFIX}_{YEAR}-{MONTH}-{DAY}{SUFFIX}$"),  # All_sensors (root)
        re.compile(
            rf"^{PREFIX}_{YEAR}-{MONTH}-{DAY}_{HOUR}-{MINUTE}$"
        ),  # All_sensors (toFTP)
        re.compile(rf"^{PREFIX} {YEAR}{MONTH}{DAY} {HOUR}{MINUTE}{SECOND}$"),  # CO2FT
        re.compile(rf"^{PREFIX}_{YEAR}{MONTH}{DAY}$"),  # GPS etc.
    )

    @property
    def package_key(self):
        key = (
            f"{self.attributes['year']}-"
            f"{self.attributes['month']}-"
            f"{self.attributes['day']}"
        )
        return key

    @property
    def target_path(self):
        if self.attributes["prefix"].startswith("All_sensors"):
            parts_list = [self.attributes["year"], "AllSensors", self.source_path.name]
            return pathlib.Path(*parts_list)
        elif self.attributes["prefix"].startswith("Watersampler"):
            parts_list = [
                self.attributes["year"],
                "Watersampler",
                self.source_path.name,
            ]
            return pathlib.Path(*parts_list)
        else:
            index = 0
            parts = list(self.source_path.parts)
            if "Working" in parts:
                index = parts.index("Working") + 1
            new_parts = parts[index:]
            if "CO2FT_A" in new_parts and "DeviceData" not in new_parts:
                new_parts = ["DeviceData", *new_parts]
            elif "HydroFIA_pH_A" in new_parts and "DeviceData" not in new_parts:
                new_parts = ["DeviceData", *new_parts]
            parts_list = [self.attributes["year"], *new_parts]
            return pathlib.Path(*parts_list)

    @staticmethod
    def from_source_file(root_directory, source_file):
        full_path = pathlib.Path(root_directory, source_file)
        if "FERRYBOX" not in str(full_path).upper():
            return None
        if "toFTP" in full_path.parts:
            return None
        if "FTP_temp" in full_path.parts:
            return None

        for PATTERN in FerryboxResourceRaw.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                resource = FerryboxResourceRaw(root_directory, source_file, attributes)
                # TODO: Move to validate
                # if str(resource.date) == '1904-01-01':
                #     logger.warning(
                #       f'Not handling file found with date 1904-01-01:
                #       {resource.absolute_source_path}'
                #     )
                #     return None
                return resource


class FerryboxResourceProcessed(Resource):
    @property
    def package_key(self):
        return datetime.datetime.now().strftime("%Y%m%d")

    @property
    def target_path(self):
        return pathlib.Path(
            self.attributes["from_year"], self.package_key, self.source_path.name
        )


class FerryboxResourceCO2(FerryboxResourceProcessed):
    PATTERNS = (
        re.compile(
            rf"^{FROM_YEAR}-{FROM_MONTH}-{FROM_DAY}_{TO_YEAR}-{TO_MONTH}-{TO_DAY}_{NAME}$"
        ),
    )

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in FerryboxResourceCO2.PATTERNS:
            name_match = PATTERN.search(source_file.stem)
            if name_match:
                attributes = name_match.groupdict()
                resource = FerryboxResourceCO2(root_directory, source_file, attributes)
                return resource


class FerryboxResourceWiski(FerryboxResourceProcessed):
    PATTERNS = (
        re.compile(
            rf"^{FROM_YEAR}-{FROM_MONTH}-{FROM_DAY}_{TO_YEAR}-{TO_MONTH}-{TO_DAY}_wiski$"
        ),
    )

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in FerryboxResourceWiski.PATTERNS:
            name_match = PATTERN.search(source_file.stem)
            if name_match:
                attributes = name_match.groupdict()
                resource = FerryboxResourceWiski(root_directory, source_file, attributes)
                return resource
