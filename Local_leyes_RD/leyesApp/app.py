import os
import json
import requests
import pandas as pd
import psycopg2
from datetime import datetime
from urllib3.exceptions import InsecureRequestWarning

from urllib3 import disable_warnings
disable_warnings(InsecureRequestWarning)

# Configuración de la conexión a la base de datos desde variables de entorno
DB_NAME = os.getenv('DB_NAME', 'dapper-prod')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'SvBuP7PQaxZ9Psp7PdVh')
DB_HOST = os.getenv('DB_HOST', 'dapper-test-repdom.cz0ywq2gy6rm.us-east-1.rds.amazonaws.com')
DB_PORT = os.getenv('DB_PORT', '5432')

conection = psycopg2.connect(
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT
)
 
# Create cursor for executing queries
cursor = conection.cursor()
 
def table_query(table_name, columns='*', where_clause=None, limit=None):
    """
    Executes a SELECT query on specified table with optional filters
    Args:
        table_name (str): Name of the table to query
        columns (str/list): Columns to select, defaults to '*'
        where_clause (str): Optional WHERE conditions
        limit (int): Optional LIMIT clause
    Returns:
        list: Query results
    """
    # Handle column names if passed as list
    if isinstance(columns, list):
        columns = ", ".join(columns)
   
    # Build base query
    sql_query = f"SELECT {columns} FROM {table_name}"
   
    # Add optional WHERE clause
    if where_clause:
        sql_query += f" WHERE {where_clause}"
   
    # Add optional LIMIT
    if limit:
        sql_query += f" LIMIT {limit}"
   
    cursor.execute(sql_query)
    return cursor.fetchall()
 
def standard_insert(df, table_name):
    """
    Performs bulk insert of DataFrame records into specified table
    Args:
        df (DataFrame): Data to insert
        table_name (str): Target table name
    Returns:
        int: Number of inserted records
    """
    try:
        # Convert DataFrame values to native Python types
        df = df.astype(object).where(pd.notnull(df), None)
       
        # Prepare column names and placeholders
        columns = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
       
        # Build insert query
        insert_query = f"""
            INSERT INTO {table_name}
            ({columns})
            VALUES ({placeholders})
        """
       
        # Convert DataFrame rows to tuples
        records_to_insert = [tuple(x) for x in df.values]
       
        # Execute bulk insert
        cursor.executemany(insert_query, records_to_insert)
        conection.commit()
       
        return len(df)
       
    except Exception as e:
        conection.rollback()
        raise Exception(f"Error inserting into {table_name}: {str(e)}")
   
def insert_regulations_component(new_ids):
    try:
        if not new_ids:
            return 0, "No new regulation IDs provided for component insertion."
 
        # Prepare components data
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['components_id'] = 3
       
        # Insert components
        components_table_name = 'dapper_regulations_regulations_component'
        inserted_count = standard_insert(id_rows, components_table_name)
       
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"
       
    except Exception as e:
        return 0, f"Error inserting regulation components: {str(e)}"

def insert_regulations_cities(new_ids):
    try:
        if not new_ids:
            return 0, "No new regulation IDs provided for component insertion."
 
        # Prepare components data
        id_rows = pd.DataFrame(new_ids, columns=['regulations_id'])
        id_rows['city_id'] = 3378
       
        # Insert components
        components_table_name = 'dapper_regulations_regulations_cities'
        inserted_count = standard_insert(id_rows, components_table_name)
       
        return inserted_count, f"Successfully inserted {inserted_count} regulation components"
       
    except Exception as e:
        return 0, f"Error inserting regulation components: {str(e)}"
 
# def insert_new_records(df):
#     """
#     Inserts new records and their components
#     Args:
#         df (DataFrame): Data to insert
#     Returns:
#         tuple: (total_records_inserted, status_message)
#     """
#     table_name = 'dapper_regulations_regulations'
 
#     try:
#         print("Verificando registros existentes en la base de datos...")
#         query = f"""
#             SELECT title, created_at
#             FROM {table_name}
#         """
#         cursor.execute(query)
#         existing_records = cursor.fetchall()
#         print(f"Registros existentes encontrados: {len(existing_records)}")
 
#         # Convertir a DataFrame para comparación
#         db_df = pd.DataFrame(
#             existing_records,
#             columns=['title', 'created_at']
#         )
 
#         # Identificar registros nuevos
#         print("Identificando registros nuevos...")
#         new_records = df[~df.apply(
#             lambda x: (
#                 (db_df['title'] == x['title']) &
#                 (db_df['created_at'].astype(str) == x['created_at'])
#             ).any(),
#             axis=1
#         )]
       
#         if new_records.empty:
#             print("No se encontraron registros nuevos para insertar.")
#             return 0, "No new records found"
       
#         print(f"Registros nuevos encontrados: {len(new_records)}")
 
#         # Insertar registros nuevos
#         print("Insertando registros nuevos...")
#         inserted_count = standard_insert(new_records, table_name)
#         print(f"{inserted_count} registros insertados correctamente.")
 
#         # Obtener IDs de los nuevos registros
#         print("Obteniendo IDs de los nuevos registros...")
#         new_ids_query = f"""
#             SELECT id
#             FROM {table_name}
#             WHERE title = ANY(%s)
#         """
#         cursor.execute(new_ids_query, (list(new_records['title']),))
#         new_ids = [row[0] for row in cursor.fetchall()]
#         print(f"IDs de nuevos registros: {new_ids}")
 
#         # Insertar componentes relacionados
#         _, component_message = insert_regulations_component(new_ids)
#         _, component_message = insert_regulations_cities(new_ids)
#         print(component_message)
 
#         message = f"Inserted {inserted_count} new records. {component_message}"
#         return inserted_count, message
       
#     except Exception as e:
#         conection.rollback()
#         print(f"Error al procesar los registros: {str(e)}")
#         return 0, f"Error processing records: {str(e)}"


def insert_new_records(df):
    """
    Inserts new records and their components if they don't already exist based on title and entity.
    Args:
        df (DataFrame): Data to insert
    Returns:
        tuple: (total_records_inserted, status_message)
    """
    table_name = 'dapper_regulations_regulations'
 
    try:
        print("Verificando registros existentes en la base de datos...")
        # Se ajusta la consulta para incluir la columna 'entity'
        cursor.execute(f"SELECT title, entity FROM {table_name}")
        existing_records = cursor.fetchall()
        print(f"Registros existentes encontrados: {len(existing_records)}")
 
        # Convertir a DataFrame para comparación, incluyendo la columna 'entity'
        existing_df = pd.DataFrame(existing_records, columns=['title', 'entity'])
 
        # Identificar registros nuevos
        new_records = df[~df.apply(
            lambda x: (
                (existing_df['title'] == x['title']) &
                (existing_df['entity'] == x['entity'])
            ).any(), axis=1
        )]
 
        if new_records.empty:
            print("No se encontraron registros nuevos para insertar.")
            return 0, "No new records found"
 
        print(f"Registros nuevos encontrados: {len(new_records)}")
 
        # Insertar registros nuevos
        print("Insertando registros nuevos...")
        placeholders = ', '.join(['%s'] * len(new_records.columns))
        columns = ', '.join(new_records.columns)
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.executemany(insert_query, [tuple(x) for x in new_records.to_numpy()])
        conection.commit()
        inserted_count = cursor.rowcount
        print(f"{inserted_count} registros insertados correctamente.")
 
        return inserted_count, "Insertion completed successfully"
 
    except Exception as e:
        conection.rollback()
        print(f"Error al procesar los registros: {str(e)}")
        return 0, f"Error processing records: {str(e)}"


def scrape_data(api_url):
    all_data = []
    page = 1
    while True:
        params = {
            'juwpfisadmin': 'false',
            'action': 'wpfd',
            'task': 'files.display',
            'view': 'files',
            'id': '78',
            'rootcat': '78',
            'orderCol': 'title',
            'orderDir': 'desc',
            'page': page
        }
        response = requests.get(api_url, params=params,verify=False)
        if response.status_code == 200:
            data = response.json()
            if not data['files']:  # Check if there are no files returned
                break
            for item in data['files']:
                title = item['post_title']
                created_at = item['created']
                linkdownload = item['linkdownload']
                all_data.append({
                    'title': title,
                    'summary': title,
                    'update_at': created_at,
                    'external_link': linkdownload,
                    'entity': "Alcaldía del Distrito Nacional",
                    'created_at': created_at,
                    'classification_id': 15,
                    'rtype_id': 23,
                    'gtype': "link",
                    'is_active': True
                })
        else:
            break
        page += 1
    return pd.DataFrame(all_data)




def lambda_handler(event, context):
    try:
        api_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"
        df = scrape_data(api_url)
        insert_new_records(df=df)
        
        return {
            "statusCode": 200,
            "body": json.dumps("Proceso completado con éxito.")
        }
    except Exception as e:
        print(str(e))
        return {
            "statusCode": 500,
            "body": json.dumps("Error durante la ejecución.")
        }
