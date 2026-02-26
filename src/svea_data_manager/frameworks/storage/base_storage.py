import logging
from abc import ABC, abstractmethod

from svea_data_manager.frameworks.package import Package

logger = logging.getLogger(__name__)


class BaseStorage(ABC):
    def write(self, package: Package, force=False):
        logger.debug(f"Writing package '{package}' to {self}.")
        if not isinstance(package, Package):
            raise TypeError(
                "package must be an instance of Package, not {}".format(type(package))
            )
        return self._write(package, force=force)

    def delete(self, package: Package):
        if not isinstance(package, Package):
            raise TypeError(
                "package must be an instance of Package, not {}".format(type(package))
            )
        return self._delete(package)

    @abstractmethod
    def _write(self, package, **kwargs) -> list:
        pass

    @abstractmethod
    def _delete(self, package):
        pass

    def __str__(self):
        return f"{self.__class__.__name__}"
