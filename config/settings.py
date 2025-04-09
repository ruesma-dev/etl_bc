# config/settings.py

"""
config/settings.py
Lee las variables de entorno para las credenciales y la configuración.
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    def __init__(self):
        self.BC_TENANT_ID = os.getenv('BC_TENANT_ID')
        self.BC_CLIENT_ID = os.getenv('BC_CLIENT_ID')
        self.BC_CLIENT_SECRET = os.getenv('BC_CLIENT_SECRET')
        self.BC_SCOPE = "https://api.businesscentral.dynamics.com/.default"
        self.BC_ENVIRONMENT = os.getenv('BC_ENVIRONMENT')
        self.BC_COMPANY_ID = os.getenv('BC_COMPANY_ID')

settings = Settings()

