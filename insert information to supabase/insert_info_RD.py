from botocore.exceptions import ClientError
import boto3
import json
import os
import pandas as pd
from supabase import create_client, Client

def process_data_in_chunks(datos, chunk_size, supabase):
    # Fetch existing dimensions from the dimension table
    def fetch_existing_dimensions():
        response = supabase.table('tabladimension').select('*').execute()
       
        # Si la respuesta no contiene datos, retornamos estructuras vacías
        if not response.data or len(response.data) == 0:
            return {}, []
 
        # Caso en que hay datos
        columns = response.data[0].keys()
        dimension_columns = [col for col in columns if col != 'id']
 
        existing_dimensions = {}
        for row in response.data:
            dimension_combination = tuple(row[col] for col in dimension_columns)
            existing_dimensions[dimension_combination] = row['id']
 
        return existing_dimensions, dimension_columns
    def get_or_add_dimension(dimension_combination):
        if dimension_combination in existing_dimensions:
            return existing_dimensions[dimension_combination]
        else:
            # Add new dimension to the dimension table
            new_dimension_data = dict(zip(dimension_columns, dimension_combination))
            print(new_dimension_data)
            response = supabase.table('tabladimension').insert(new_dimension_data).execute()
            new_id = response.data[0]['id']
            existing_dimensions[dimension_combination] = new_id
            return new_id
 
    def create_dimension_combination(row):
        return tuple(row[col] for col in dimension_columns)
   
    existing_dimensions, dimension_columns = fetch_existing_dimensions()
    errores = []  # To track errors
    total_chunks = (len(datos) + chunk_size - 1) // chunk_size
    # Define columns to keep in the final upsert
    columns_to_keep = ['Fecha', 'Valor', 'id']
   
    for i in range(total_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        chunk_data = datos[start_index:end_index]
 
        # Prepare data for upsert
        for row in chunk_data:
            dimension_combination = create_dimension_combination(row)
            dimension_id = get_or_add_dimension(dimension_combination)
            row['id'] = dimension_id
 
        # Drop columns not in the columns_to_keep
        chunk_data_filtered = [{k: row[k] for k in columns_to_keep} for row in chunk_data]
 
        # Upsert data into the main table
        try:
            response = supabase.table('datos_light').upsert(chunk_data_filtered).execute()
            #print(response)
        except Exception as e:
            errores.append(str(e))
 
    return f"Terminamos errores: {', '.join(errores)}"

# def process_data_in_chunks(datos, chunk_size, supabase):
    
#     datos = datos.to_dict(orient='records')
#     print("datos", datos)

#     # Fetch existing dimensions from the dimension table
#     def fetch_existing_dimensions():
#         response = supabase.table('tabladimension').select('*').execute()
        
#         # Si la respuesta no contiene datos, retornamos estructuras vacías
#         if not response.data or len(response.data) == 0:
#             print("No existing dimensions found.")
#             return {}, []
 
#         # Caso en que hay datos
#         columns = response.data[0].keys()
#         dimension_columns = [col for col in columns if col != 'id']
#         # print(f"Existing dimension columns: {dimension_columns}")
 
#         existing_dimensions = {}
#         for row in response.data:
#             dimension_combination = tuple(row[col] for col in dimension_columns)
#             existing_dimensions[dimension_combination] = row['id']
 
#         return existing_dimensions, dimension_columns
    
#     def get_or_add_dimension(dimension_combination):
#         if dimension_combination in existing_dimensions:
#             return existing_dimensions[dimension_combination]
#         else:
#             # Add new dimension to the dimension table
#             new_dimension_data = dict(zip(dimension_columns, dimension_combination))
#             print(f"Adding new dimension data: {new_dimension_data}")
#             response = supabase.table('tabladimension').insert(new_dimension_data).execute()
#             print(f"Adding dimension response: {response}")
#             new_id = response.data[0]['id']
#             existing_dimensions[dimension_combination] = new_id
#             return new_id
 
#     def create_dimension_combination(row):
#         return tuple(row[col] for col in dimension_columns)
   
#     existing_dimensions, dimension_columns = fetch_existing_dimensions()
#     errores = []  # To track errors
#     total_chunks = (len(datos) + chunk_size - 1) // chunk_size
#     # print("Total de chunks", total_chunks)
#     # print("Dimesion columns", dimension_columns)
#     # Define columns to keep in the final upsert
#     columns_to_keep = ['Fecha', 'Valor', 'id']
   
#     for i in range(total_chunks):
#         start_index = i * chunk_size
#         end_index = (i + 1) * chunk_size
#         chunk_data = datos[start_index:end_index]
#         # print(f"Processing chunk {i+1} of {total_chunks}")
 
#         # Prepare data for upsert
#         for row in chunk_data:
#             dimension_combination = create_dimension_combination(row)
#             dimension_id = get_or_add_dimension(dimension_combination)
#             row['id'] = dimension_id
 
#         # Drop columns not in the columns_to_keep
#         chunk_data_filtered = [{k: row[k] for k in columns_to_keep} for row in chunk_data]
 
#         # Upsert data into the main table
#         try:
#             response = supabase.table('datos_light').upsert(chunk_data_filtered).execute()
#             print(f"Upsert chunk response: {response.status_code}")
#         except Exception as e:
#             errores.append(str(e))
 
#     return f"Terminamos errores: {', '.join(errores)}"
 
def get_secret():
    secret_name = "RepDom-DB"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = json.loads(get_secret_value_response['SecretString'])
    return secret

secretos = get_secret()
url_supabase: str = secretos['supabase_URL_RD']
key: str = secretos['supabase_key_RD']

def process_and_upload(file_path, supabase):
    try:
        data = pd.read_csv(file_path)        
        data.rename(columns={
            'Código ISO':'Codigo_ISO',
            'Sub-Categoría':'Sub-Categoria',
            'Categoría': 'Categoria',
            'País': 'Pais',
            'Pais_Destino': 'Pais_Origen',
            'Régimen Aduanero': 'Regimen_Aduanero',
            'Código Sección': 'Codigo_Seccion',
            'Sección': 'Seccion',
            'Código Arancel': 'Codigo_Arancel',
            'Código Capítulo': 'Codigo_Capitulo',
            'Código Partida': 'Codigo_Partida',
            'Capítulo': 'Capitulo',
            'Región': 'Region',
            'Vía Transporte': 'Via_Transporte',
            'Colecturía ID': 'Colecturia_ID',
            'Colecturía': 'Colecturia'
        }, inplace=True)

        data.drop(columns=['Mes', 'Columna_desagregacion'], inplace=True)
        datos = data.to_dict(orient='records')
        response = process_data_in_chunks(datos=datos,chunk_size=10000,supabase=supabase)
        if response.error:
            print(f"Error al insertar datos de {file_path}: {response.error}")
        else:
            print(f"Datos insertados correctamente para {file_path}")
    except pd.errors.ParserError:
        print(f"Error al parsear el archivo: {file_path}")
    except KeyError as e:
        print(f"Error de columna faltante en {file_path}: {str(e)}")
    except Exception as e:
        print(f"Error inesperado en {file_path}: {str(e)}")

def connect_to_supabase(url: str, key: str) -> Client:
    return create_client(url, key)

if __name__ == "__main__":
    secretos=get_secret()
    url_supabase: str = secretos['supabase_URL_RD']
    key: str = secretos['supabase_key_RD'] 
    supabase = connect_to_supabase(url_supabase, key)
    # Listar todos los archivos en la carpeta especificada y procesarlos uno por uno
    folder_path = 'data/Economico RD'
    files = [os.path.join(folder_path, file) for file in os.listdir(folder_path) if file.endswith('.csv')]
    for file_path in files:
        process_and_upload(file_path=file_path, supabase=supabase)
    

 
