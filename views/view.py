"""
Super class for all views
"""

from abc import ABC, abstractmethod
from typing import Dict


class View(ABC):
    
    def run(self) -> None:
        pass

    def check_conn(self) -> bool:
        return True