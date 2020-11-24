from abc import ABC, abstractmethod


class AbstractIdentifyCreator(ABC):
    @abstractmethod
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError


class CryptoOversoldCreator(AbstractIdentifyCreator):
    def factory_method(self, *args, **kwargs):
        pass

