from svea_data_manager.frameworks import Instrument


class SveaDataManager:

    def __init__(self, instruments = []):
        self._instruments = {}

        for instrument in instruments:
            self.register_instrument(instrument)


    def register_instrument(self, instrument):
        if not isinstance(instrument, Instrument):
            raise TypeError('Only instances of Instrument can be registered')
        
        instrument_type = type(instrument).__name__
        
        if instrument_type in self._instruments.keys():
            raise ValueError(
                'Exactly one instance of the same instrument can be '
                'registered. An instance of %s is already registered.'
                % instrument_type
            )
        
        self._instruments[instrument_type] = instrument
    
    def unregister_instrument(self, instrument):
        if not isinstance(instrument, Instrument):
            raise TypeError('Only instances of Instrument can be unregistered')

        instrument_type = type(instrument).__name__

        if not instrument in self._instruments.values():

            if instrument_type in self._instruments.keys():
                raise ValueError(
                    'Given instance of instrument is not a registered instrument. '
                    'However, another instance of %s is registered. '
                    'Did you mean to unregister another instance of %s?'
                    % (instrument_type, instrument_type)
                )
            else:
                raise ValueError(
                    'Given instance of instrument is not a registered instrument, '
                    'neither are any other instance of %s.' % instrument_type
                )
        
        del self._instruments[instrument_type]
 
    @property
    def instruments(self):
        return list(self._instruments.values())


    def read_packages(self):
        for instrument in self.instruments:
            instrument.read_packages()

    def transform_packages(self):
        for instrument in self.instruments:
            instrument.transform_packages()

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

  
    @classmethod
    def from_config(cls, config):
        instance = cls()

        from svea_data_manager import instruments
        for instrument_type in config:
            try:
                instrument_cls = instruments.__getattribute__(instrument_type)
            except AttributeError:
                raise ValueError(
                    'Could not resolve instrument class for key %s '
                    'found in config.' % instrument_type
                )

            instrument = instrument_cls(config[instrument_type])
            instance.register_instrument(instrument)

        return instance
