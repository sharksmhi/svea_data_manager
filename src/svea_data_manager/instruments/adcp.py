import datetime
import logging
import pathlib
import re
from typing import ClassVar

from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import FileStorage
from svea_data_manager.instruments import exceptions

CRUISE_NOT_NEEDED_AFTER_DATE = datetime.date(2023, 1, 1)

CRUISE_MAPPING = {
    (datetime.date(2022, 1, 10), datetime.date(2022, 1, 13)): "1",  # January
    (datetime.date(2022, 2, 8), datetime.date(2022, 2, 10)): "3",  # February
    (datetime.date(2022, 3, 17), datetime.date(2022, 3, 21)): "5",  # Mars
    (datetime.date(2022, 4, 20), datetime.date(2022, 4, 25)): "7",  # April
    (datetime.date(2022, 5, 19), datetime.date(2022, 5, 26)): "10",  # May
    (datetime.date(2022, 6, 13), datetime.date(2022, 6, 17)): "11",  # June
    (datetime.date(2022, 7, 12), datetime.date(2022, 7, 17)): "12",  # Juli
    (datetime.date(2022, 8, 12), datetime.date(2022, 8, 19)): "13",  # August
}


logger = logging.getLogger(__name__)

INSTRUMENT = r"(?P<instrument>ADCP\w+)"
SHIP = r"(?P<ship>\d{2}\D{2})"
YEAR = r"(?P<year>\d{4})"
CRUISE = r"(?P<cruise>\d{2})"
COUNTER = r"(?P<counter>.+)"
NR = r"(?P<nr>.+)"
MONTH_STRING = r"(?P<month_string>\D+)"


def get_start_date_from_log_file(path):
    with open(path) as fid:
        for line in fid:
            date_strings = re.findall(r"\d{4}/\d{2}/\d{2}", line)
            if not date_strings:
                continue
            return datetime.datetime.strptime(date_strings[0], "%Y/%m/%d").date()


def get_start_date_from_cruise_info_file(path):
    with open(path) as fid:
        for line in fid:
            if line.startswith("CRUISE DATES"):
                date_strings = re.findall(r"\d{4}/\d{2}/\d{2}", line)
                if not date_strings:
                    raise ValueError
                return datetime.datetime.strptime(date_strings[0], "%Y/%m/%d").date()


def get_cruise_number_for_date(date):
    if date > CRUISE_NOT_NEEDED_AFTER_DATE:
        return True
    for key, value in CRUISE_MAPPING.items():
        fr, to = key
        if fr <= date <= to:
            return value
    return False


class Adcp(Instrument):
    name = "ADCP"
    desc = "ADCP monitoring from Svea"

    def __init__(self, config):
        super().__init__(config)
        self._storage = FileStorage(self._config["target_directory"])
        self._package_key_attributes = {}

    def prepare_resource(self, source_file):
        resource = AdcpResourceProcessed.from_source_file(
            self.source_directory, source_file
        )
        if not resource:
            resource = AdcpResourceRaw.from_source_file(
                self.source_directory, source_file
            )
        if not resource:
            resource = AdcpResourceReadme.from_source_file(
                self.source_directory, source_file
            )
        # if not resource.attributes['instrument'].startswith('ADCP'):
        #     resource.attributes['instrument'] = (
        #       'ADCP' + resource.attributes['instrument']
        #     )
        if not resource:
            return
        # self._set_ship(resource)
        # self._set_cruise(resource)
        return resource

    def _set_ship(self, resource):
        resource.attributes["ship"] = self.config.get("attributes", {}).get("ship", None)
        # if not resource.attributes.get('ship'):
        #     # if not self.config.get('attributes', {}).get('ship'):
        #     #     msg = f'No ship information for file {resource.absolute_source_path}'
        #     #     logger.error(msg)
        #     #     raise exceptions.ShipError(msg)
        #     resource.attributes['ship'] = self.config.get(
        #       'attributes', {}).get('ship', None)

    def _set_cruise(self, resource):
        cruise = self.config.get("attributes", {}).get("cruise", None)
        if not cruise:
            return
        resource.attributes["cruise"] = cruise
        # if not resource.attributes.get('cruise'):
        #     if not self.config.get('attributes', {}).get('cruise'):
        #         msg = f'No cruise information for file {resource.absolute_source_path}'
        #         logger.error(msg)
        #         raise exceptions.ShipError(msg)
        #     resource.attributes['cruise'] = self.config.get(
        #       'attributes', {}).get('cruise', None)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def transform_packages(self, **kwargs):
        self._package_key_attributes = {}
        for package in self.packages:
            attributes = self.transform_package(package, **kwargs)
            if str(package) == "readme":
                continue
            self._package_key_attributes[str(package)] = attributes

    def transform_package(self, package, **kwargs):
        date = None
        metadata = kwargs.get("metadata", {})
        resource = {}
        for resource in package.resources:
            if not date:
                if resource.source_path.stem == "cruise_info":
                    date = get_start_date_from_cruise_info_file(
                        resource.absolute_source_path
                    )
                elif resource.source_path.suffix.upper() == ".LOG":
                    date = get_start_date_from_log_file(resource.absolute_source_path)
                if date:
                    cruise = get_cruise_number_for_date(date)
                    if cruise is True:
                        break
                    elif cruise is False:
                        if metadata.get("cruise"):
                            msg = (
                                f"Cruise for package {package!s} if set from the outside "
                                f"to {metadata.get('cruise')}"
                            )
                            logger.warning(msg)
                        else:
                            msg = (
                                f"No internal mapping for cruise was found "
                                f"for package {package!s}"
                            )
                            logger.error(msg)
                            raise exceptions.CruiseError(msg)
                    else:
                        msg = (
                            f"Cruise for package {package!s} if set by internal mapping "
                            f"from {metadata.get('cruise')} to {cruise}"
                        )
                        logger.warning(msg)
                        metadata["cruise"] = cruise
                    break

        for resource in package.resources:
            resource.attributes.update(metadata)
            package._package_key = resource.package_key
        return resource.attributes.copy()

    def write_package(self, package):
        logger.info(f"Writing package '{package}' to {self._storage}.")
        # return self._storage.write(package, self._config.get('force', False))
        if str(package) == "readme":
            for key, attr in self._package_key_attributes.items():
                attr["package_key"] = key
                for resource in package.resources:
                    resource.attributes.update(attr)
                self._storage.write(package, self._config.get("force", False))
        else:
            return self._storage.write(package, self._config.get("force", False))


class AdcpResource(Resource):
    INSTRUMENT_MAPPING: ClassVar[dict] = {
        "ADCPWHM600": "ADCPWH600",
        "WH600": "ADCPWH600",
        "WHM600": "ADCPWH600",
        "OS150": "ADCPOS150",
    }

    @property
    def package_key(self):
        return (
            f"{self.attributes['instrument']}_{self.attributes['ship']}_{self.attributes['year']}"
            f"_{self.attributes['cruise'].zfill(2)}"
        )


class AdcpResourceRaw(AdcpResource):
    PATTERNS = (
        re.compile(rf"^{INSTRUMENT}_{SHIP}_{YEAR}_{CRUISE}_{COUNTER}_{NR}$"),
        re.compile(rf"^{INSTRUMENT}_{SHIP}_{YEAR}_{CRUISE}_{NR}$"),
        re.compile(rf"^{INSTRUMENT}_SMHI_{MONTH_STRING}_{YEAR}_{NR}$"),
        re.compile(rf"^{INSTRUMENT}_SMHI_{MONTH_STRING}{YEAR}_{NR}$"),
    )

    @property
    def target_path(self):
        parts_list = [
            self.attributes["instrument"],
            self.attributes["year"],
            self.package_key,
            "raw",
            self.source_path.name,
        ]
        # Assuring instrument sub folder in instrument class
        return pathlib.Path(*parts_list)

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in AdcpResourceRaw.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                attributes["suffix"] = source_file.suffix
                attributes["instrument"] = AdcpResourceRaw.INSTRUMENT_MAPPING.get(
                    attributes["instrument"], attributes["instrument"]
                )
                resource = AdcpResourceRaw(root_directory, source_file, attributes)
                return resource
        logger.debug(f"No patterns match for file: {source_file}")


class AdcpResourceProcessed(AdcpResource):
    VALID_PATH_IDS: ClassVar[dict] = {
        "OS_LTA": "ADCPOS150",
        "WH_LTA": "ADCPWH600",
    }

    SUB_DIRS = ("BB", "NB")

    PATTERNS = (
        re.compile(rf"{INSTRUMENT}_{SHIP}_{YEAR}_{CRUISE}_processed"),
        re.compile(rf"{INSTRUMENT}_{SHIP}_{YEAR}_{CRUISE}_utdata"),
    )

    @property
    def target_path(self):
        root = pathlib.Path(
            self.attributes["instrument"],
            self.attributes["year"],
            self.package_key,
            "processed",
        )
        subdir = None
        for part in self.source_path.parts:
            if part.upper() in AdcpResourceProcessed.SUB_DIRS:
                subdir = part.upper()
                break
        if subdir:
            root = pathlib.Path(root, subdir)
        if self.attributes["suffix"] == ".nc":
            path = pathlib.Path(root, self.source_path.name)
            # path = pathlib.Path(root, f'{self.package_key}{self.attributes["suffix"]}')
        else:
            file_name = f"{self.package_key}_{self.source_path.name}"
            if self.attributes["suffix"] == ".png":
                path = pathlib.Path(root, "plots", file_name)
            else:
                path = pathlib.Path(root, "info", file_name)
        return path

    @staticmethod
    def from_source_file(root_directory, source_file):
        for PATTERN in AdcpResourceProcessed.PATTERNS:
            name_match = PATTERN.search(str(pathlib.Path(root_directory, source_file)))

            if name_match:
                attributes = name_match.groupdict()
                attributes["suffix"] = source_file.suffix
                for key, value in AdcpResourceProcessed.VALID_PATH_IDS.items():
                    if key in str(pathlib.Path(root_directory, source_file)):
                        attributes["instrument"] = value
                        break
                attributes["instrument"] = AdcpResourceProcessed.INSTRUMENT_MAPPING.get(
                    attributes["instrument"], attributes["instrument"]
                )

                # if not attributes.get('instrument'):
                #     return

                resource = AdcpResourceProcessed(root_directory, source_file, attributes)
                return resource


class AdcpResourceReadme(AdcpResource):
    @property
    def package_key(self):
        return "readme"

    @property
    def target_path(self):
        parts_list = [
            self.attributes["instrument"],
            self.attributes["year"],
            self.attributes["package_key"],
            self.source_path.name.lower(),
        ]
        return pathlib.Path(*parts_list)

    @staticmethod
    def from_source_file(root_directory, source_file):
        if "readme" in str(source_file):
            resource = AdcpResourceReadme(root_directory, source_file)
            return resource
        logger.debug(f"No patterns match for file: {source_file}")
