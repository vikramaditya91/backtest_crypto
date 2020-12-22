import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class AbstractTimeStepSimulateCreator(ABC):
    def factory_method(self, *args, **kwargs):
        raise NotImplementedError

    def calculate_end_of_time_value(self,
                                    *args,
                                    **kwargs):
        concrete = self.factory_method(*args, **kwargs)
        return concrete.calculate_end_of_run_value()


class SimpleMarketBuyLimitSellSimulationCreator(AbstractTimeStepSimulateCreator):
    def factory_method(self, *args, **kwargs):
        return


class AbstractTimestepSimulatorConcrete(ABC):
    @abstractmethod
    def calculate_end_of_run_value(self, *Args, **kwargs):
        raise NotImplementedError


class SimpleMarketBuyLimitSellSimulatorConcrete(AbstractTimestepSimulatorConcrete):
    def __init__(self,
                 potential_coin_data_struct,
                 potential_coin_high_cutoff,
                 potential_coin_low_cutoff,
                 coin_history_values,
                 timedelta_to_achieve_success,
                 percentage_increase,
                 start_simulation_at
                 ):
        self.potential_coin_data_struct = potential_coin_data_struct
        self.high_cutoff = potential_coin_high_cutoff
        self.low_cutoff = potential_coin_low_cutoff
        self.coin_history_values = coin_history_values
        self.timedelta_to_achieve_success = timedelta_to_achieve_success
        self.percentage_increase = percentage_increase
        self.start_simulation_at = start_simulation_at

    def calculate_end_of_run_value(self, *args, **kwargs):
        pass


def calculate_end_of_run_simulation_value(creator: AbstractTimeStepSimulateCreator,
                                          *args,
                                          **kwargs):
    return creator.calculate_end_of_time_value(*args,
                                               **kwargs)
