# infrastructure/business_central/bc_repository.py

"""
infrastructure/business_central/bc_repository.py
Implementación del repositorio de Business Central usando BCClient.
"""
from typing import Dict, Any
from domain.repositories.interfaces import BusinessCentralRepositoryInterface
from infrastructure.business_central.bc_client import BCClient

class BCRepository(BusinessCentralRepositoryInterface):
    """
    Implementa las operaciones para obtener datos de Business Central
    a través del BCClient.
    """
    def __init__(self, bc_client: BCClient):
        self.bc_client = bc_client

    def get_companies(self) -> Dict[str, Any]:
        return self.bc_client.fetch_companies()

    def get_entities(self) -> Dict[str, Any]:
        return self.bc_client.fetch_entities()

    def get_customers(self) -> Dict[str, Any]:
        return self.bc_client.fetch_customers()

    def get_currency(self) -> Dict[str, Any]:
        return self.bc_client.fetch_currency()

    def get_financial_details(self) -> Dict[str, Any]:
        return self.bc_client.fetch_financial_details()
