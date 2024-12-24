import json
import boto3
from botocore.exceptions import ClientError
import openai
from supabase import create_client
from dateutil import parser


def convert_fecha_consulta(data):
    # Define una función para manejar la conversión de fechas
    def parse_date(date_str):
        try:
            # Intenta parsear el string de fecha al primer día del mes
            return parser.parse(date_str, default=pd.Timestamp(year=1900, month=1, day=1)).strftime('%Y-%m-%d')
        except ValueError:
            # Si hay un error en el parseo, retorna None o una cadena vacía
            return None

    if 'fecha_consulta' in data.columns:
        data['fecha_consulta'] = data['fecha_consulta'].apply(parse_date)
    return data

def get_secret():
    secret_name = "Openai"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
 
    secret = json.loads(get_secret_value_response['SecretString'])
    return secret
 
def upload_data(data, entidad):
    secretos = get_secret()
    url_supabase: str = secretos['supabase_url']
    key: str = secretos['supabase_key']
 
    supabase = create_client(url_supabase, key)

    if 'fecha_consulta' in data.columns and pd.api.types.is_datetime64_any_dtype(data['fecha_consulta']):
        data['fecha_consulta'] = data['fecha_consulta'].apply(lambda x: x.isoformat() if not pd.isnull(x) else None)

    # data = convert_fecha_consulta(data)
    # datos = data.to_dict(orient="records")
 
    # Obtener nombres existentes para evitar duplicados por entidad y nombre
    response = (
        supabase.table("agendas_regulatorias")
        .select("nombre")
        .eq("entidad", entidad)
        .execute()
    )
 
    nombres_existentes = [d["nombre"] for d in response.data] if response.data else []
   
    # Convertir el DataFrame a una lista de diccionarios
    datos = data.to_dict(orient="records")
   
    # Filtrar registros que ya existen en la base de datos
    datos = [d for d in datos if d["nombre"] not in nombres_existentes]
 
    errores = []
    chunk_size = 10000
    total_chunks = (len(datos) + chunk_size - 1) // chunk_size
 
    for i in range(total_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_data = datos[start_index:end_index]
 
        try:
            response = supabase.table("agendas_regulatorias").upsert(chunk_data).execute()
            print(response)
        except Exception as e:
            errores.append(str(e))
 
    return {"errores": errores, "añadidos": len(datos)}


import pandas as pd

if __name__ == '__main__':
    file_path = "data/test.xlsx"
    entidad = 'Ministerio de Hacienda y Crédito Público'

    try:
        # Leer el archivo Excel
        data = pd.read_excel(file_path)

        # Verificar si el DataFrame está vacío
        if data.empty:
            print("El archivo Excel no contiene datos.")
        else:
            # Llamar a la función upload_data con los datos y la entidad
            resultado = upload_data(data, entidad)

            # Mostrar el resultado del proceso
            print("Proceso completado.")
            print(f"Errores: {resultado['errores']}")
            print(f"Número de registros añadidos: {resultado['añadidos']}")
    except FileNotFoundError:
        print("No se encontró el archivo especificado. Por favor, verifica la ruta.")
    except Exception as e:
        print(f"Se produjo un error: {str(e)}")
