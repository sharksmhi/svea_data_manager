import logging
import os
import string

import yaml

from svea_data_manager.frameworks import Instrument
from svea_data_manager import helpers
from svea_data_manager.sdm_event import post_event

logger = logging.getLogger(__name__)


class SveaDataManager:

    def __init__(self, instruments=[]):
        self._instruments = {}

        for instrument in instruments:
            self.register_instrument(instrument)

    def register_instrument(self, instrument):
        if not isinstance(instrument, Instrument):
            msg = 'Only instances of Instrument can be registered'
            logger.error(msg)
            raise TypeError(msg)
        
        instrument_type = type(instrument).__name__
        
        if instrument_type in self._instruments.keys():
            msg = f'Exactly one instance of the same instrument can be ' \
                  f'registered. An instance of {instrument_type} is already registered.'
            logger.error(msg)
            raise ValueError(msg)
        
        self._instruments[instrument_type] = instrument
        post_event('log', dict(msg=f'Instrument registered: {instrument_type}'))
    
    def unregister_instrument(self, instrument):
        if not isinstance(instrument, Instrument):
            raise TypeError('Only instances of Instrument can be unregistered')

        instrument_type = type(instrument).__name__

        if instrument not in self._instruments.values():

            if instrument_type in self._instruments.keys():
                msg = f'Given instance of instrument is not a registered instrument. ' \
                      f'However, another instance of {instrument_type} is registered. ' \
                      f'Did you mean to unregister another instance of {instrument_type}?'
                logger.error(msg)
                raise ValueError(msg)

            else:
                msg = f'Given instance of instrument is not a registered instrument, neither are any other instance of {instrument_type}.'
                logger.error(msg)
                raise ValueError(msg)

        del self._instruments[instrument_type]

        post_event('log', dict(msg=f'Instrument unregistered: {instrument_type}'))
 
    @property
    def instruments(self):
        return list(self._instruments.values())

    def read_packages(self, **kwargs):
        post_event('before_read_packages')
        post_event('log', dict(msg=f'Reading packages...'))
        for instrument in self.instruments:
            instrument.read_packages(**kwargs)
        post_event('after_read_packages')

    def transform_packages(self, **kwargs):
        post_event('before_transform_packages')
        post_event('log', dict(msg=f'Transforming packages...'))
        for instrument in self.instruments:
            instrument.transform_packages(**kwargs)
        post_event('after_transform_packages')

    def write_packages(self):
        post_event('before_write_packages')
        post_event('log', dict(msg=f'Writing packages...'))
        for instrument in self.instruments:
            instrument.write_packages()
        helpers.clear_temp_dir()
        post_event('after_write_packages')

    def run(self):
        post_event('log', dict(msg=f'Running all'))
        # Step 1 - extract packages for each registered instrument.
        self.read_packages()
        # Step 2 - transform packages for each registered instrument.
        self.transform_packages()
        # Step 3 - load packages for each registered instrument.
        self.write_packages()

    @classmethod
    def from_config(cls, config):
        instance = cls()

        import svea_data_manager.instruments

        instruments = {instrument.name.upper(): instrument for instrument in Instrument.__subclasses__()}

        for instrument_type in config:
            try:
                instrument_cls = instruments[instrument_type.upper()]
            except AttributeError:
                msg = f'Could not resolve instrument class for key {instrument_type} found in config.'
                logger.error(msg)
                raise ValueError(msg)

            instrument = instrument_cls(config[instrument_type])
            instance.register_instrument(instrument)

        return instance

    @classmethod
    def from_yaml(cls, config_path, config_vars={}):
        config_content = ''
        with open(config_path, 'r', encoding='utf8') as config_file:
            config_content = config_file.read()

        env_vars = {
            env_key: env_val for env_key, env_val
            in os.environ.items() if env_key.startswith('SVEA_')
        }

        config_template = string.Template(config_content)

        config = yaml.safe_load(
            config_template.safe_substitute(env_vars, **config_vars)
        )

        return cls.from_config(config)
