"""
Super class for all views
"""

from abc import ABC, abstractmethod
from typing import Dict


class View(ABC):
    
    @abstractmethod
    def show(self) -> None:
        pass

    @abstractmethod
    def check_conn(self) -> bool:
        return True