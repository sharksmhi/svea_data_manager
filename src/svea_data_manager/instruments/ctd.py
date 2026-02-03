import logging
import re
from pathlib import Path
from typing import Self

from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.frameworks.resource import Resource
from svea_data_manager.frameworks.storage import FileStorage, SubversionStorage
from svea_data_manager.instruments import exceptions

logger = logging.getLogger(__name__)

SHIPS = {"77_10": "77SE"}


class CTD(Instrument):
    name = "CTD"
    desc = "Conductivity, temperature and depth monitoring from Svea"

    def __init__(self, config):
        super().__init__(config)

        if "subversion_repo_url" in self._config:
            self._storage = SubversionStorage(self._config["subversion_repo_url"])
        elif "target_directory" in self._config:
            self._storage = FileStorage(self._config["target_directory"])
        else:
            raise exceptions.InstrumentConfigurationError(
                "Missing required configuration. Either 'subversion_repo_url' or "
                "'target_directory' must be set."
            )

    def prepare_resource(self, source_file: Path):
        return CTDResource.from_source_file(self.source_directory, source_file)

    def get_package_key_for_resource(self, resource):
        return resource.package_key

    def write_package(self, package):
        logger.info("Writing package %s to subversion repo" % package)
        return self._storage.write(package, self._config.get("force", False))


class CTDResource(Resource):
    RAW_FILE_SUFFIXES = (
        ".bl",
        ".btl",
        ".hdr",
        ".hex",
        ".ros",
        ".xmlcon",
        ".xml",
        ".zip",
    )
    PREFIX_OPTIONAL = r"(?P<prefix>u|d)?"
    PREFIX_UPCAST = r"(?P<prefix>u)?"
    INSTRUMENT = r"(?P<instrument>SBE\d{2})"
    INSTRUMENT_NUMBER = r"(?P<instrument_number>\d{4})"
    YEAR = r"(?P<year>\d{4})"
    MONTH = r"(?P<month>\d{2})"
    DAY = r"(?P<day>\d{2})"
    HOUR = r"(?P<hour>\d{2})"
    MINUTE = r"(?P<minute>\d{2})"
    SHIP_UNDERSCORE = r"(?P<ship>\d{2}_\w{2})"
    SHIP = r"(?P<ship>\d{2}\w{2})"
    CRUISE = r"(?P<cruise>\d{2})"
    SERNO = r"(?P<serno>\d{4})"

    PATTERNS = (
        re.compile(
            rf"^{PREFIX_OPTIONAL}{INSTRUMENT}_{INSTRUMENT_NUMBER}_"
            rf"{YEAR}{MONTH}{DAY}_{HOUR}{MINUTE}_{SHIP_UNDERSCORE}_{SERNO}$"
        ),
        re.compile(
            rf"^{PREFIX_UPCAST}{INSTRUMENT}_{INSTRUMENT_NUMBER}_"
            rf"{YEAR}{MONTH}{DAY}_{HOUR}{MINUTE}_{SHIP}_{CRUISE}_{SERNO}$"
        ),
        re.compile(
            rf"^{PREFIX_UPCAST}{INSTRUMENT}_{INSTRUMENT_NUMBER}_"
            rf"{YEAR}{MONTH}{DAY}_{HOUR}{MINUTE}_{SHIP}_{CRUISE}_{SERNO}_psa_config$"
        ),
    )

    @property
    def date_str(self):
        return self.attributes["year"] + self.attributes["month"] + self.attributes["day"]

    @property
    def time_str(self):
        return self.attributes["hour"] + self.attributes["minute"]

    @property
    def ship(self):
        ship_id = self.attributes["ship"]
        return SHIPS.get(ship_id, ship_id)

    @property
    def cruise(self):
        return self.attributes.get("cruise", "00")

    @property
    def package_key(self):
        return (
            f"{self.attributes['instrument']}_{self.attributes['instrument_number']}_"
            f"{self.date_str}_{self.time_str}_"
            f"{self.ship}_{self.cruise}_{self.attributes['serno']}"
        )

    @property
    def target_path(self):
        path = Path(self.attributes["year"])

        if self.source_path.suffix == ".cnv":
            if self.attributes.get("prefix"):
                if self.attributes["prefix"].lower() == "u":
                    path = path / "cnv" / "upcast"
                elif self.attributes["prefix"].lower() == "d":
                    path = path / "cnv" / "downcast"
            else:
                path = path / "cnv"
        elif self.source_path.suffix.lower() in CTDResource.RAW_FILE_SUFFIXES:
            path = path / "raw"
        elif self.source_path.suffix == ".txt":
            pass
        file_name = f"{self.source_path.stem}{self.source_path.suffix.lower()}"
        if self.attributes.get("suffix"):
            file_name = self.attributes["suffix"].lower() + file_name[1:]
        return path.joinpath(file_name)

    @classmethod
    def from_source_file(cls, root_directory: Path, source_file: Path) -> Self | None:
        if source_file.suffix.lower() not in (
            *CTDResource.RAW_FILE_SUFFIXES,
            ".cnv",
            ".txt",
        ):
            return None
        for PATTERN in CTDResource.PATTERNS:
            name_match = PATTERN.search(source_file.stem)

            if name_match:
                attributes = name_match.groupdict()
                return cls(root_directory, source_file, attributes)
        return None
