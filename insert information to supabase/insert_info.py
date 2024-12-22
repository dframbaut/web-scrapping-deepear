import json
import boto3
from botocore.exceptions import ClientError
import openai
from supabase import create_client
 
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
    file_path = 'insert information to supabase/data/Ministerio_comercio_turismo_cultura_Con_Tema.xlsx'
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
