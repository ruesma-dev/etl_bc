"""
application/use_cases/bc_use_cases.py
Casos de uso para interactuar con Business Central y transformaciones.
"""

from domain.repositories.interfaces import BusinessCentralRepositoryInterface
from domain.services.transform_service import TransformService
from typing import Dict, Any, List
import pandas as pd

class BCUseCases:
    def __init__(self, bc_repository: BusinessCentralRepositoryInterface, transform_service: TransformService):
        self.bc_repository = bc_repository
        self.transform_service = transform_service

    def get_entities(self) -> Dict[str, Any]:
        """
        Devuelve el JSON de entidades disponibles en BC.
        """
        return self.bc_repository.get_entities()

    def get_companies(self) -> Dict[str, Any]:
        """
        Devuelve el JSON de las empresas en BC.
        """
        return self.bc_repository.get_companies()

    def get_customers(self) -> Dict[str, Any]:
        """
        Devuelve el JSON de clientes en BC.
        """
        return self.bc_repository.get_customers()

    def export_customers_to_csv(self, customers_json: Dict[str, Any], file_path: str = "customers_export.csv") -> None:
        """
        Convierte el JSON de clientes en un DataFrame y lo exporta a CSV.
        """
        df_customers = pd.DataFrame(customers_json.get('value', []))
        df_customers.to_csv(file_path, index=False, encoding='utf-8')
