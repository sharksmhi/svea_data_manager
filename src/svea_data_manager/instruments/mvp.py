import logging
import pathlib
import re

from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import SubversionStorage
from svea_data_manager.instruments import exceptions

logger = logging.getLogger(__name__)


class MVP(Instrument):
    name = "MVP"
    desc = "MVP monitoring from Svea"

    def __init__(self, config):
        super().__init__(config)
        if "subversion_repo_url" not in self._config:
            raise exceptions.InstrumentConfigurationError(
                "Missing required configuration subversion_repo_url."
            )
        self._storage = SubversionStorage(self._config["subversion_repo_url"])

    def prepare_resource(self, source_file):
        return MVPResource.from_source_file(self.source_directory, source_file)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info("Writing package %s to subversion repo" % package)
        return self._storage.write(package, self._config.get("force", False))


class MVPResource(Resource):
    PREFIX_OPTIONAL = r"(?P<prefix>.{1})?"
    INSTRUMENT = r"(?P<instrument>MVP)"
    YEAR = r"(?P<year>\d{4})"
    MONTH = r"(?P<month>\d{2})"
    DAY = r"(?P<day>\d{2})"
    HOUR = r"(?P<hour>\d{2})"
    MINUTE = r"(?P<minute>\d{2})"
    SECOND = r"(?P<second>\d{2})"
    TRANSECT_DASHED = r"(?P<transect>.+-.+)"
    TRANSECT = r"(?P<transect>.+)"
    ENDING = r"(?P<ending>_.*)?"

    PATTERNS = (
        re.compile(
            rf"^{PREFIX_OPTIONAL}{INSTRUMENT}_"
            rf"{YEAR}-{MONTH}-{DAY}_{HOUR}{MINUTE}{SECOND}_{TRANSECT_DASHED}$",
            re.I,
        ),
        re.compile(
            rf"^{PREFIX_OPTIONAL}{INSTRUMENT}_"
            rf"{YEAR}-{MONTH}-{DAY}_{HOUR}{MINUTE}{SECOND}_{TRANSECT}$",
            re.I,
        ),
        # TODO: if ending is anything else than "" or "_", this re will never catch.
        #       Also, code requires attribute transect
        re.compile(
            rf"^{PREFIX_OPTIONAL}{INSTRUMENT}_"
            rf"{YEAR}-{MONTH}-{DAY}_{HOUR}{MINUTE}{SECOND}{ENDING}$",
            re.I,
        ),
    )

    @property
    def package_key(self):
        return (
            f"{self.attributes['year']}-{self.attributes['month']}-"
            f"{self.attributes['day']} {self.attributes['hour']}:"
            f"{self.attributes['minute']}:{self.attributes['second']}"
        )

    @property
    def target_path(self):
        parts = list(self.source_path.parts)
        cut = "unknown"
        for i, part in enumerate(parts):
            if part.startswith("SMHI_"):
                cut = parts[i + 1]
                break
        if "RAWDATA" in parts:
            parts_list = [self.attributes["year"], cut, "raw", self.source_path.name]
            return pathlib.Path(*parts_list)
        if self.source_path.suffix == ".cnv":
            if self.attributes["prefix"] == "u":
                parts_list = [
                    self.attributes["year"],
                    cut,
                    "cnv",
                    "upcast",
                    self.source_path.name,
                ]
            elif self.attributes["prefix"] == "d":
                parts_list = [
                    self.attributes["year"],
                    cut,
                    "cnv",
                    "downcast",
                    self.source_path.name,
                ]
            else:
                parts_list = [
                    self.attributes["year"],
                    cut,
                    "cnv",
                    self.source_path.name,
                ]
            return pathlib.Path(*parts_list)
        if self.source_path.suffix == ".jpg":
            parts_list = [
                self.attributes["year"],
                cut,
                "cnv",
                "downcast",
                "plot",
                self.source_path.name,
            ]
            return pathlib.Path(*parts_list)
        return pathlib.Path("annat", self.source_path.name)  # Temporary while testing

    @staticmethod
    def from_source_file(root_directory, source_file):
        path_str = str(pathlib.Path(root_directory, source_file)).upper()
        if "MVP" not in path_str:
            return None
        if "SMHI_" not in path_str:
            return None
        for PATTERN in MVPResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)
            if name_match:
                attributes = name_match.groupdict()
                if not attributes.get("transect") and "RAWDATA" in source_file.parts:
                    attributes["transect"] = source_file.parent.name
                attributes["transect"] = attributes["transect"].upper()
                attributes["suffix"] = source_file.suffix
                resource = MVPResource(root_directory, source_file, attributes)
                return resource
