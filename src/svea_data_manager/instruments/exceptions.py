from svea_data_manager.exceptions import SveaDataManagerError


class InstrumentError(SveaDataManagerError):
    pass


class CruiseError(InstrumentError):
    pass


class ShipError(InstrumentError):
    pass


class NoInstrumentInformationError(InstrumentError):
    """Cannot find information about instrument"""

    pass


class TargetPathExistsError(InstrumentError):
    """The target path already exists"""

    pass
