# domain/repositories/interfaces.py

"""
domain/repositories/interfaces.py
Contiene la interfaz para interactuar con Business Central.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any

class BusinessCentralRepositoryInterface(ABC):
    """
    Contrato para obtener datos (empresas, entidades, clientes, currency, detalles financieros)
    desde Business Central.
    """

    @abstractmethod
    def get_companies(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_entities(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_customers(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_currency(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_financial_details(self) -> Dict[str, Any]:
        pass
