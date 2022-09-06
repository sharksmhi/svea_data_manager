from svea_data_manager.frameworks import Instrument

import logging

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
 
    @property
    def instruments(self):
        return list(self._instruments.values())

    def read_packages(self):
        for instrument in self.instruments:
            instrument.read_packages()

    def transform_packages(self, **kwargs):
        for instrument in self.instruments:
            instrument.transform_packages(**kwargs)

    def write_packages(self):
        for instrument in self.instruments:
            instrument.write_packages()

    def run(self):
        # Step 1 - extract packages for each registered instrument.
        self.read_packages()
        # Step 2 - transform packages for each registered instrument.
        self.transform_packages()
        # Step 3 - load packages for each registered instrument.
        self.write_packages()

    def write_report(self, directory):
        for inst in self._instruments.values():
            inst.write_report(directory)

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
