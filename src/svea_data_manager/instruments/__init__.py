from svea_data_manager.instruments.adcp import ADCP
from svea_data_manager.instruments.ctd import CTD
from svea_data_manager.instruments.ferrybox import Ferrybox
from svea_data_manager.instruments.ifcb import IFCB
from svea_data_manager.instruments.ifcb_classification import IFCBclassification
from svea_data_manager.instruments.mvp import MVP

INSTRUMENT_MAP: dict = {
    ADCP.__name__.upper(): ADCP,
    CTD.__name__.upper(): CTD,
    Ferrybox.__name__.upper(): Ferrybox,
    IFCB.__name__.upper(): IFCB,
    IFCBclassification.__name__.upper(): IFCBclassification,
    MVP.__name__.upper(): MVP,
}
