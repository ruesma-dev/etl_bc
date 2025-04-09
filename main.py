"""
main.py
Punto de entrada de la aplicación. Construye la lista de Steps ETL y la ejecuta.
"""

from infrastructure.business_central.bc_client import BCClient
from infrastructure.business_central.bc_repository import BCRepository

from domain.services.transform_service import TransformService
from application.use_cases.bc_use_cases import BCUseCases

from interface_adapters.controllers.etl_controller import ETLController
from interface_adapters.controllers.pipeline_steps import ListCompaniesStep

def main():
    # 1. Infraestructura
    bc_client = BCClient()
    bc_repository = BCRepository(bc_client)

    # 2. Servicios / Casos de uso
    transform_service = TransformService()
    bc_use_cases = BCUseCases(bc_repository, transform_service)

    # 3. Crear Steps
    list_companies_step = ListCompaniesStep(bc_use_cases)
    # Podrías definir más steps en el futuro (ListCustomersStep, etc.)

    steps = [
        list_companies_step
        # [En el futuro: + otros steps]
    ]

    # 4. Controlador con la pipeline de steps
    controller = ETLController(steps)

    # 5. Ejecutar
    controller.run_etl_process()

if __name__ == "__main__":
    main()
