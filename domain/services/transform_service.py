# domain/services/transform_service.py

"""
domain/services/transform_service.py
Lógica de transformaciones (limpieza, merges) usando pandas.
"""
import pandas as pd

class TransformService:
    """
    Encapsula la lógica de transformaciones de datos con pandas
    (ejemplo: filtrar, hacer merges, etc.).
    """
    def __init__(self):
        # Configuraciones globales de pandas (opcional)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)

    def transform_customer_financial(self, customers_json: dict, financial_json: dict):
        """
        Toma el JSON de clientes y detalles financieros, realiza
        filtrados y joins, y devuelve un DataFrame resultante.
        """
        df_customers = pd.DataFrame(customers_json['value'])
        df_customer_financial = pd.DataFrame(financial_json['value'])

        # Columnas deseadas
        df_deseado1 = [
            'id', 'number', 'displayName',
            'addressLine1', 'city', 'state', 'postalCode', 'currencyId'
        ]
        df_deseado2 = [
            'id', 'number', 'balance', 'totalSalesExcludingTax', 'overdueAmount'
        ]

        df_filtrado = df_customers[df_deseado1]
        df_filtrado2 = df_customer_financial[df_deseado2]

        # Merge
        df_join = df_filtrado.merge(df_filtrado2, how='left', on='id')

        # Comprobar que las columnas number_x y number_y sean idénticas
        if (df_join['number_x'] == df_join['number_y']).all():
            df_join.drop(columns=['number_y'], inplace=True)
            df_join.rename(columns={'number_x': 'number'}, inplace=True)

        return df_join
