import requests
from bs4 import BeautifulSoup
import json
import boto3
from supabase import create_client
from datetime import datetime
from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import re
from sre_constants import NOT_LITERAL
from google.colab import files

# Load environment variables from .env file
load_dotenv()

# Configuración de Secretos y Conexión a Supabase
def get_secret():
    secret_name = "Openai"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response['SecretString'])
    except Exception as e:
        print(f"Error al obtener los secretos: {str(e)}")
        raise e
import requests
from bs4 import BeautifulSoup
import re
import json
import pandas as pd
import unicodedata
from google.colab import files

def scrapping_ids(url):
    """
    Función principal que realiza todo el flujo de scrapping:
    - Extrae IDs primarios desde una URL.
    - Realiza una iteración recursiva para obtener IDs secundarios.
    - Descarga y procesa archivos asociados con esos IDs.
    - Genera un archivo CSV con los resultados.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }

    # Extraer IDs primarios desde la URL
    response = requests.get(url, headers=headers, verify=False)
    soup = BeautifulSoup(response.text, 'html.parser')
    a_tags = soup.find_all('a', class_='wpfdcategory catlink')
    ids_list = [tag.get('data-idcat') for tag in a_tags]
    print("IDCAT encontrados:", ids_list)

    # Parámetros base para solicitudes
    base_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"
    base_params = {
        "juwpfisadmin": "false",
        "action": "wpfd",
        "task": "categories.display",
        "view": "categories",
        "top": "108"
    }

    # Función auxiliar para extraer term_taxonomy_id
    def extraer_term_taxonomy_id(base_url, params, exclude_ids):
        try:
            response = requests.get(base_url, params=params, headers=headers, verify=False)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            page_text = soup.get_text()
            term_taxonomy_ids = re.findall(r'"term_taxonomy_id":(\d+)', page_text)
            return [id_val for id_val in term_taxonomy_ids if id_val not in exclude_ids]
        except Exception as e:
            print(f"Error al procesar ID {params['id']}: {e}")
            return []

    # Iteración para obtener IDs secundarios
    processed_ids = set(ids_list)
    idcat_values2 = []
    iteration_count = 1
    while True:
        print(f"\nIteración {iteration_count}")
        nuevos_ids = []
        for id_val in ids_list:
            params = base_params.copy()
            params["id"] = id_val
            ids_extraidos = extraer_term_taxonomy_id(base_url, params, processed_ids)
            nuevos_ids.extend(ids_extraidos)
        nuevos_ids = list(set(nuevos_ids) - processed_ids)
        if not nuevos_ids:
            print("No se encontraron nuevos IDs. Finalizando.")
            break
        idcat_values2.append(nuevos_ids)
        processed_ids.update(nuevos_ids)
        ids_list = nuevos_ids
        iteration_count += 1
    final_idcat_values = list(set([item for sublist in idcat_values2 for item in sublist] + list(processed_ids)))
    print("\nResultados finales:")
    print(f"Lista combinada de IDCAT: {final_idcat_values}")

    # Scraping de archivos por IDs secundarios
    api_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"
    nombres, link, fecha_pub, fecha_vigor, epigrafe = [], [], [], [], []

    def normalize_text(text):
        text = unicodedata.normalize('NFD', text)
        return ''.join(c for c in text if unicodedata.category(c) != 'Mn').lower()

    def fetch_files_by_id(api_url, ids, base_params):
        all_results = []
        for current_id in ids:
            print(f"Procesando ID: {current_id}...")
            params = base_params.copy()
            params["id"] = current_id
            params["rootcat"] = current_id
            page = 1
            while True:
                params["page"] = page
                response = requests.get(api_url, params=params, verify=False)
                if response.status_code == 200:
                    data = response.json()
                    archivos = data.get("files", [])
                    if not archivos:
                        break
                    for archivo in archivos:
                        post_title = archivo.get("post_title", "Sin título")
                        normalized_title = normalize_text(post_title)
                        nombres.append(normalized_title)
                        epigrafe.append(normalized_title)
                        fecha_pub.append(archivo.get("created_time", "Sin fecha"))
                        fecha_vigor.append(archivo.get("modified_time", "Sin fecha"))
                        link.append(archivo.get("linkdownload", "Sin enlace"))
                        all_results.append({
                            "post_title": normalized_title,
                            "created_time": archivo.get("created_time", "Sin fecha"),
                            "modified_time": archivo.get("modified_time", "Sin fecha"),
                            "linkdownload": archivo.get("linkdownload", "Sin enlace")
                        })
                    page += 1
                else:
                    break
        return all_results

    resultados = fetch_files_by_id(api_url, final_idcat_values, base_params)

    # Generar DataFrame y CSV
    data = {
        "title": nombres,
        "external_link": link,
        "created_at": fecha_pub,
        "update_at": fecha_vigor,
        "summary": epigrafe
    }
    df = pd.DataFrame(data)
    df['Tipo'] = None
    df['entity'] = 'Alcaldia del distrito Nacional'
    df['classification_id'] = None
    df['rtype_id'] = '13'
    df['gtype'] = 'link'
    df['is_active'] = True
    columnas_ordenadas = ['title', 'summary', 'update_at', 'external_link', 'entity', 'created_at', 'classification_id', 'rtype_id', 'gtype', 'is_active']
    df = df[columnas_ordenadas]
    df.to_csv("planificacion_y_desarrollo.csv", index=False, encoding='utf-8-sig')
    print("Archivo CSV generado exitosamente.")

# Create database connection using environment variables
conection = psycopg2.connect(
    dbname=os.getenv('DB_NAME'), 
    user=os.getenv('DB_USERNAME'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
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

def insert_new_records(df):
    """
    Inserts new records and their components
    Args:
        df (DataFrame): Data to insert
    Returns:
        tuple: (total_records_inserted, status_message)
    """
    table_name = 'dapper_regulations_regulations'

    try:
        # Get existing records for comparison
        query = f"""
            SELECT title, created_at 
            FROM {table_name}
        """
        cursor.execute(query)
        existing_records = cursor.fetchall()
        
        # Convert to DataFrame for comparison
        db_df = pd.DataFrame(
            existing_records, 
            columns=['title', 'created_at']
        )
        
        # Identify truly new records
        new_records = df[~df.apply(
            lambda x: (
                (db_df['title'] == x['title']) & 
                (db_df['created_at'].astype(str) == x['created_at'])
            ).any(), 
            axis=1
        )]
        
        if new_records.empty:
            return 0, "No new records found"
        
        # Insert records using standard_insert
        inserted_count = standard_insert(new_records, table_name)
        
        # Get the new regulation IDs
        new_ids_query = f"""
            SELECT id 
            FROM {table_name} 
            WHERE title = ANY(%s)
        """
        cursor.execute(new_ids_query, (list(new_records['title']),))
        new_ids = [row[0] for row in cursor.fetchall()]

        # Insert regulation components and combine messages
        component_count, component_message = insert_regulations_component(new_ids)
    
        message = f"Inserted {inserted_count} new records. {component_message}"
        
        return inserted_count, message
        
    except Exception as e:
        conection.rollback()
        return 0, f"Error processing records: {str(e)}"

    

def lambda_handler(event, context):
    secretos = get_secret()
    url_supabase = secretos['supabase_url']
    key = secretos['supabase_key']
    supabase = create_client(url_supabase, key)
    url = "https://adn.gob.do/transparencia/planificacion-y-desarrollo/'"
    data = scrapping_ids(url)
    if data:
        response = insert_new_records(df=data)
        print(f"Datos insertados en Supabase: {response}")
    return {"statusCode": 200, "body": json.dumps("Scraping completado y datos cargados")}
