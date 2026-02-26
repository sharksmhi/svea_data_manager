import importlib
import logging
import os
import pkgutil
import string
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from svea_data_manager import helpers
from svea_data_manager.frameworks.instrument import Instrument
from svea_data_manager.sdm_event import post_event

logger = logging.getLogger(__name__)


class SveaDataManager:
    def __init__(self, instruments: list | None = None):
        self._instruments = {}

        for instrument in instruments or []:
            self.register_instrument(instrument)

    def register_instrument(self, instrument: Instrument):
        if not isinstance(instrument, Instrument):
            msg = "Only instances of Instrument can be registered"
            logger.error(msg)
            raise TypeError(msg)

        instrument_type = type(instrument).__name__

        if instrument_type in self._instruments.keys():
            msg = (
                f"Exactly one instance of the same instrument can be "
                f"registered. An instance of {instrument_type} is already registered."
            )
            logger.error(msg)
            raise ValueError(msg)

        self._instruments[instrument_type] = instrument
        post_event("log", dict(msg=f"Instrument registered: {instrument_type}"))

    def unregister_instrument(self, instrument):
        if not isinstance(instrument, Instrument):
            raise TypeError("Only instances of Instrument can be unregistered")

        instrument_type = type(instrument).__name__

        if instrument not in self._instruments.values():
            if instrument_type in self._instruments.keys():
                msg = (
                    f"Given instance of instrument is not a registered instrument. "
                    f"However, another instance of {instrument_type} is registered. "
                    f"Did you mean to unregister another instance of {instrument_type}?"
                )
                logger.error(msg)
                raise ValueError(msg)

            else:
                msg = (
                    f"Given instance of instrument is not a registered instrument, "
                    f"neither are any other instance of {instrument_type}."
                )
                logger.error(msg)
                raise ValueError(msg)

        del self._instruments[instrument_type]

        post_event("log", dict(msg=f"Instrument unregistered: {instrument_type}"))

    @property
    def instruments(self):
        return list(self._instruments.values())

    def read_packages(self, **kwargs):
        logger.info("Reading packages...")
        post_event("before_read_packages")
        post_event("log", dict(msg="Reading packages..."))
        for instrument in self.instruments:
            instrument.read_packages(**kwargs)
        post_event("after_read_packages")

    def transform_packages(self, **kwargs):
        logger.info("Transforming packages...")
        post_event("before_transform_packages")
        post_event("log", dict(msg="Transforming packages..."))
        for instrument in self.instruments:
            instrument.transform_packages(**kwargs)
        post_event("after_transform_packages")

    def write_packages(self):
        logger.info("Writing packages...")
        post_event("before_write_packages")
        post_event("log", dict(msg="Writing packages..."))
        combined_writes_by_directory = defaultdict(list)
        for instrument in self.instruments:
            writes_by_directory = instrument.write_packages()
            for key, writes in writes_by_directory.items():
                combined_writes_by_directory[key] += writes

        helpers.clear_temp_dir()
        post_event("after_write_packages")
        _log_writes(combined_writes_by_directory)

    def run(self):
        post_event("log", {"msg": "Running all"})
        # Step 1 - extract packages for each registered instrument.
        self.read_packages()
        # Step 2 - transform packages for each registered instrument.
        self.transform_packages()
        # Step 3 - load packages for each registered instrument.
        self.write_packages()

    @classmethod
    def from_config(cls, config):
        instance = cls()

        instrument_map = get_instrument_map()
        for instrument_type in config:
            try:
                instrument_cls = instrument_map[instrument_type.upper()]
            except KeyError:
                msg = (
                    f"Could not resolve instrument class "
                    f"for key {instrument_type} found in config."
                )
                logger.error(msg)
                raise ValueError(msg)

            instrument = instrument_cls(config[instrument_type])
            instance.register_instrument(instrument)

        return instance

    @classmethod
    def from_yaml(cls, config_path, config_vars: dict | None = None):
        config_vars = config_vars or {}
        config_content = ""
        with open(config_path, "r", encoding="utf8") as config_file:
            config_content = config_file.read()

        env_vars = {
            env_key: env_val
            for env_key, env_val in os.environ.items()
            if env_key.startswith("SVEA_")
        }

        config_template = string.Template(config_content)

        config = yaml.safe_load(config_template.safe_substitute(env_vars, **config_vars))

        return cls.from_config(config)


@lru_cache
def get_instrument_map() -> dict[str, type[Instrument]]:
    instruments_dir = Path(__file__).parent / "instruments"
    for module_info in pkgutil.iter_modules([str(instruments_dir)]):
        if module_info.name != "exceptions":
            importlib.import_module(f"{__package__}.instruments.{module_info.name}")
    return {cls.__name__.upper(): cls for cls in Instrument.__subclasses__()}


def _log_writes(combined_writes_by_directory: defaultdict[Any, list]):
    number_of_files = sum(len(paths) for paths in combined_writes_by_directory.values())
    number_of_directories = len(combined_writes_by_directory)

    directories = {
        "/".join(top_levels)
        for directory in combined_writes_by_directory
        if (top_levels := directory.parts[:2]) and len(top_levels) == 2
    }

    ordered_directories = ", ".join(sorted(directories))
    main_directories = f" Main directories: {ordered_directories}" if directories else ""
    logger.info(
        f"Wrote {number_of_files} files in {number_of_directories} directories."
        + main_directories
    )
