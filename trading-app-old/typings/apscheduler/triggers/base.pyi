from abc import ABCMeta, abstractmethod


class BaseTrigger(metaclass=ABCMeta):
    @abstractmethod
    def get_next_fire_time(self, previous_fire_time, now): ...  # type: ignore
