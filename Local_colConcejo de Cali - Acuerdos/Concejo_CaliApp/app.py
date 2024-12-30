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


def scrapping1(base_url):

    headers = {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
      }

    # Crear listas para almacenar los datos
    Nombres = []
    Epigrafes = []
    Fecha_publicacion = []
    Fecha_vigor = []
    Link = []

    # Número de páginas
    num_paginas = 2

    # Iterar por cada página
    for pagina in range(1, num_paginas + 1):
        # Construir la URL para cada página
        url = f"{base_url}?genPagDocs={pagina}"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')

            # Encontrar los elementos <p>, <span> y <div> necesarios
            p_tags = soup.find_all('p', class_='col-md-9')
            span_tags = soup.find_all('span')
            div_contents = soup.find_all('div', class_='pdf icono')
            div_contents2 = soup.find_all('div', class_='col-md-9')

            # Extraer enlaces de los <div> con clase "pdf icono"
            for div_content in div_contents2:
              a_tag = div_content.find('a')
              if a_tag:
                # Buscar la etiqueta <strong> dentro del <a>
                strong_tag = a_tag.find('strong')
                texto_enlace = strong_tag.get_text(strip=True)  # Extraer el texto dentro de <strong>
                Nombres.append(texto_enlace)  # Agregar tanto el texto como el enlace a la lista

            # Extraer datos de los elementos <p>
            for p in p_tags:
                texto_completo = p.get_text(strip=True)
                if ':' in texto_completo:
                    Epigrafe = texto_completo.split(':', 1)[1].strip()  # Después de ":"
                    Epigrafes.append(Epigrafe)

            # Extraer datos de los elementos <span>
            for span in span_tags:
                texto = span.get_text(strip=True)
                if 'Expedición:' in texto:
                    Fecha_1 = texto.split(':', 1)[1].strip()  # Después de ":"
                    Fecha_publicacion.append(Fecha_1)
                if 'Publicación:' in texto:
                    Fecha_2 = texto.split(':', 1)[1].strip()  # Después de ":"
                    Fecha_vigor.append(Fecha_2)

            # Extraer enlaces de los <div> con clase "pdf icono"
            for div_content in div_contents:
                a_tag = div_content.find('a')
                if a_tag and a_tag.get('href'):
                    link = a_tag['href']
                    Link.append(link)

        else:
            print(f"Error al acceder a la página {pagina}. Código de estado: {response.status_code}")
    return Nombres, Epigrafes, Fecha_publicacion, Fecha_vigor, Link


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
    base_url = "https://www.concejodecali.gov.co/documentos/5033/acuerdos-2024/"
    data = scrapping1(base_url)
    if data:
        response = insert_new_records(df=data)
        print(f"Datos insertados en Supabase: {response}")
    return {"statusCode": 200, "body": json.dumps("Scraping completado y datos cargados")}
