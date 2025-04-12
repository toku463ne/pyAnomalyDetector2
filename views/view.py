"""
Super class for all views
"""

from abc import ABC, abstractmethod
from typing import Dict


class View(ABC):
    
    def show(self, trend_start=0, history_start=0, history_end=0) -> None:
        pass

    def check_conn(self) -> bool:
        return True