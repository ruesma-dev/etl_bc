# infrastructure/business_central/bc_client.py

"""
infrastructure/business_central/bc_client.py
Maneja la conexión y autenticación con Business Central (obtención del token y peticiones).
"""
import requests
from config.settings import settings

class BCClient:
    """
    Clase que encapsula la autenticación y peticiones a Business Central.
    """
    def __init__(self):
        self.tenant_id = settings.BC_TENANT_ID
        self.client_id = settings.BC_CLIENT_ID
        self.client_secret = settings.BC_CLIENT_SECRET
        self.scope = settings.BC_SCOPE
        self.environment = settings.BC_ENVIRONMENT
        self.company_id = settings.BC_COMPANY_ID
        self._access_token = None

    def _fetch_access_token(self):
        """
        Obtiene el token de acceso (client_credentials).
        """
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scope': self.scope
        }

        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        return response.json()['access_token']

    def get_access_token(self):
        """
        Devuelve el token de acceso, lo refresca si no existe.
        """
        if not self._access_token:
            self._access_token = self._fetch_access_token()
        return self._access_token

    def fetch_companies(self):
        url = f"https://api.businesscentral.dynamics.com/v2.0/{self.environment}/api/v2.0/companies"
        return self._call_get(url)

    def fetch_entities(self):
        url = f"https://api.businesscentral.dynamics.com/v2.0/{self.environment}/api/V2.0/"
        return self._call_get(url)

    def fetch_customers(self):
        url = f"https://api.businesscentral.dynamics.com/v2.0/{self.environment}/api/V2.0/companies({self.company_id})/customers"
        return self._call_get(url)

    def fetch_currency(self):
        url = f"https://api.businesscentral.dynamics.com/v2.0/{self.environment}/api/V2.0/companies({self.company_id})/currencies"
        return self._call_get(url)

    def fetch_financial_details(self):
        url = f"https://api.businesscentral.dynamics.com/v2.0/{self.environment}/api/V2.0/companies({self.company_id})/customerFinancialDetails"
        return self._call_get(url)

    def _call_get(self, url):
        """
        Método interno para GET requests con el token.
        """
        token = self.get_access_token()
        headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json'
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
