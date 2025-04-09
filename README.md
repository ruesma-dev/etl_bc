ETL Business Central
Este proyecto implementa un proceso ETL que se conecta a Microsoft Business Central para obtener datos de entidades (empresas, clientes, etc.), con la idea de ir ampliando sus funcionalidades de forma incremental. Se utiliza una arquitectura limpia (Clean Architecture) siguiendo principios SOLID, separando responsabilidades en distintas capas.

Estructura de Carpetas
arduino
Copiar
Editar
etl_bc/
├── .env
├── requirements.txt
├── config/
│   └── settings.py
├── domain/
│   ├── repositories/
│   │   └── interfaces.py
│   └── services/
│       └── transform_service.py
├── infrastructure/
│   └── business_central/
│       ├── bc_client.py
│       └── bc_repository.py
├── application/
│   └── use_cases/
│       └── bc_use_cases.py
├── interface_adapters/
│   └── controllers/
│       ├── etl_controller.py
│       └── pipeline_steps.py
└── main.py
Descripción de cada carpeta
.env: Archivo con las credenciales y configuración sensible (tenant, client_id, etc.). No lo incluyas en repositorios públicos.

requirements.txt: Lista de dependencias y librerías requeridas (requests, python-dotenv, pandas, etc.).

config/

settings.py: Carga las variables del .env y expone los ajustes de configuración (tenant, environment, credenciales, etc.).

domain/

repositories/interfaces.py: Define la interfaz BusinessCentralRepositoryInterface, contratando los métodos para obtener datos de BC.

services/transform_service.py: Contiene la lógica de transformación (limpieza, merges, etc.) aplicada a los datos (normalmente usando pandas).

infrastructure/business_central/

bc_client.py: Se encarga de la autenticación (OAuth2) y la comunicación real con la API de Business Central (peticiones GET/POST, etc.).

bc_repository.py: Implementa la interfaz de repositorio definida en domain/ usando bc_client.py.

application/use_cases/

bc_use_cases.py: Casos de uso que invocan métodos del repositorio e invocan servicios de dominio (por ejemplo, get_companies, get_customers, transform_customers_financial, etc.).

interface_adapters/controllers/

pipeline_steps.py: Contiene clases “step” que definen pasos concretos del ETL (por ejemplo, ListCompaniesStep). Cada paso implementa una interfaz (ETLStepInterface) con un método run(context).

etl_controller.py: Controlador principal que orquesta la ejecución secuencial de los “steps” (un pipeline), manteniendo un context compartido entre pasos.

main.py: Punto de entrada del proyecto. Aquí se inyectan las dependencias (repositorio, casos de uso, pasos) y se lanza el proceso ETL.

Requisitos Previos
Python 3.8+

pip (o un entorno virtual como venv).

Configuración e Instalación
Clonar este repositorio:

bash
Copiar
Editar
git clone https://github.com/tuorg/etl_bc.git
cd etl_bc
Crear (opcional) un entorno virtual y activarlo:

bash
Copiar
Editar
python -m venv .venv
source .venv/bin/activate  # En Linux/Mac
.venv\Scripts\activate     # En Windows
Instalar dependencias:

bash
Copiar
Editar
pip install -r requirements.txt
Configurar el archivo .env:

ini
Copiar
Editar
# .env
BC_TENANT_ID=XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX
BC_CLIENT_ID=YYYYYYYY-YYYY-YYYY-YYYY-YYYYYYYYYYYY
BC_CLIENT_SECRET=<YOUR_CLIENT_SECRET>
BC_ENVIRONMENT=production
BC_COMPANY_ID=ZZZZZZZZ-ZZZZ-ZZZZ-ZZZZ-ZZZZZZZZZZZZ
Ajusta estos valores según tu tenant, entorno y credenciales de Business Central.

Ejecución
Una vez configurado el .env y el entorno virtual (opcional), simplemente ejecuta:

bash
Copiar
Editar
python main.py
El flujo ETL inicial hará lo siguiente:

Conectarse a Business Central (usando bc_client.py).

Listar las empresas disponibles e imprimir sus nombres e IDs en consola.

(En el futuro se irán añadiendo pasos adicionales.)

Extensión del ETL (Pipeline Steps)
El código está diseñado para que cada etapa del proceso ETL sea un “step” independiente que se encadena en el ETLController. Por ejemplo, para listar clientes, crear un CSV, etc. Cada step se define en pipeline_steps.py (o en un archivo adicional) y se registra en main.py.

Esto facilita la extensibilidad del proyecto, ya que para añadir nuevas funciones solo creas un nuevo paso y lo inyectas al pipeline.

Principios y Patrones
Clean Architecture / SOLID:

Separa la lógica de negocio (domain/) de la lógica de aplicación (application/use_cases/) y de los detalles de infraestructura (infrastructure/).

Inyecta dependencias en main.py para mantener acoplamiento bajo.

Pipeline Steps:

Cada paso (por ejemplo, ListCompaniesStep) implementa una interfaz con un método run(context) que lee y/o modifica un contexto compartido, permitiendo un flujo ETL encadenado.

Uso de pandas para transformaciones:

La lógica de limpieza y merges se ubica en transform_service.py, quedando separada de la obtención de datos.
