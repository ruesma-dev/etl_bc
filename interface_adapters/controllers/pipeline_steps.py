"""
interface_adapters/controllers/pipeline_steps.py

Define clases para cada etapa (step) del pipeline ETL.
Cada step implementa un método run(context).
"""

from typing import Any, Dict
from application.use_cases.bc_use_cases import BCUseCases

class ETLStepInterface:
    """
    Interfaz base para cada paso del pipeline ETL.
    """
    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ejecuta el paso, modificando o leyendo 'context' según sea necesario.
        Devuelve el context actualizado.
        """
        raise NotImplementedError

class ListCompaniesStep(ETLStepInterface):
    """
    Paso que obtiene la lista de empresas desde Business Central,
    la imprime y la guarda en el context.
    """
    def __init__(self, bc_use_cases: BCUseCases):
        self.bc_use_cases = bc_use_cases

    def run(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        1. Llamar a bc_use_cases para obtener empresas
        2. Imprimirlas
        3. Almacenar en context para pasos posteriores
        """
        companies_json = self.bc_use_cases.get_companies()
        companies_list = companies_json.get("value", [])

        print("Empresas disponibles en Business Central:")
        for comp in companies_list:
            print(f"- {comp['name']} (ID: {comp['id']})")

        # Guardamos en el contexto por si otro paso lo necesitara
        context["companies"] = companies_list
        return context
