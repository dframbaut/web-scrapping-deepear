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
def scrapping (base_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }

    # Crear listas para almacenar los datos
    Nombres = []
    Epigrafes = []
    Fecha_publicacion = []
    Fecha_vigor = []
    Link = []
    Link2=[]
    Tipos=[]
    Entidades=[]



    # Realizar la solicitud HTTP a la página base con las cabeceras
    response = requests.get(base_url, headers=headers)

    # Número de páginas
    num_paginas = 102

    # Iterar por cada página
    for pagina in range(1, num_paginas + 1):
        # Construir la URL para cada página
        url = f"{base_url}?pagina={pagina}"
        response = requests.get(url, headers=headers)

        # Comprobar si la solicitud fue exitosa
        if response.status_code == 200:
            # Parsear el contenido HTML de la página
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Buscar todas las tablas con la clase 'tabla'
            tables = soup.find_all('table', class_='tabla')

            if tables:
                # Recorrer todas las tablas encontradas
                for table_index, tabla4 in enumerate(tables, start=1):
                    # Buscar todas las filas del cuerpo de la tabla
                    rows = tabla4.find_all('tr')
                    
                    # Recorrer todas las filas de cada tabla
                    for row_index, row in enumerate(rows, start=1):
                        # Buscar todas las celdas de la fila
                        cells = row.find_all('td')
                        
                        if cells:  # Verificar que haya celdas en la fila
                            # Tomar la última celda
                            last_cell = cells[-1]
                            
                            # Buscar el enlace dentro de la última celda
                            link_tag = last_cell.find('a')
                            
                            if link_tag and link_tag.get('href'):  # Verificar si el <a> tiene un atributo href
                                relative_link = link_tag['href']
                                full_link = urljoin(base_url, relative_link)  # Combina el URL base con el enlace relativo
                                Link.append(full_link)
                                print(f"Tabla {table_index}, Fila {row_index}: Enlace encontrado -> {full_link}")
                            else:
                                print(f"Tabla {table_index}, Fila {row_index}: No se encontró enlace en la última celda.")
                
                # Imprimir todos los enlaces extraídos de las tablas
                print("\nEnlaces extraídos de todas las tablas:")
                print(Link)
                
                # Navegar y hacer scraping en cada uno de los enlaces extraídos
                for link in Link:
                    print(f"\nNavegando al enlace: {link}")
                    
                    # Hacer una solicitud HTTP a la página del enlace
                    response_link = requests.get(link, headers=headers)
                    if response_link.status_code == 200:
                        # Parsear el contenido HTML de la página enlazada
                        soup_link = BeautifulSoup(response_link.text, 'html.parser')
                        
                        # Verificar si se encontró una tabla en la página enlazada
                        tables_link = soup_link.find_all('table')

                        #escoger tabla en la pagina
                        if len(tables_link) >= 4: 
                            table4 = tables_link[3]

                            # Buscar todas las filas del cuerpo de la tabla
                            rows = table4.find_all('tr')
                                  
                            # Recorrer todas las filas de cada tabla
                            for row_index, row in enumerate(rows, start=1):
                                entidad="Alcaldia de Cali"
                                Entidades.append(entidad)
                                # Buscar todas las celdas de la fila
                                cells = row.find_all('td')
                                
                                if cells:  # Verificar que haya celdas en la fila
                                    # Extraer información de cada celda
                                    if len(cells) >= 1:  # Asegurarse de que haya al menos 1 celda
                                        dato1 = cells[0].get_text(strip=True)
                                        # Procesar dato1 para tipo
                                        Tipos.append(dato1)
                                        print(f"Tipo: {dato1}")
                                    
                                    if len(cells) >= 2:  # Asegurarse de que haya al menos 2 celdas
                                        dato2 = cells[1].get_text(strip=True)
                                        # Procesar dato2 para nombre
                                        Nombres.append(dato2)
                                        print(f"Nombre: {dato2}")
                                    
                                    if len(cells) >= 3:  # Asegurarse de que haya al menos 3 celdas
                                        dato3 = cells[2].get_text(strip=True)
                                        # Procesar dato3 para fecha de publicacion y fecha de vigor
                                        Fecha_publicacion.append(dato3)
                                        Fecha_vigor.append(dato3)
                                        print(f"Fecha: {dato3}")
                                    
                                    if len(cells) >= 4:  # Asegurarse de que haya al menos 4 celdas
                                        dato4 = cells[3].get_text(strip=True)
                                        # Procesar dato4 para epígrafe
                                        Epigrafes.append(dato4)
                                        print(f"Epigrafe: {dato4}")
                                    
                                    if len(cells) >= 8:  # Asegurarse de que haya al menos 8 celdas
                                        dato5 = cells[7]
                                        a_tag2 = dato5.find('a', onmouseup=True)      
                                        if a_tag2:
                                            # Obtener el valor del atributo 'onmouseup'
                                            onmouseup_value = a_tag2['onmouseup']
                                            
                                            # Extraer el enlace con una expresión regular
                                            match = re.search(r"MM_openBrWindow\('([^']+)'", onmouseup_value)
                                            if match:
                                                link = match.group(1)
                                                #Extraer solo el url 
                                                if 'https://' in link:
                                                  start = link.find('https://')
                                                  end = link.find("'", start)  # Buscar el cierre de la URL
                                                  dato5= link[start:end]
                                                  # Procesar dato5 como el link del documento
                                                  Link2.append(dato5)
                                                  print(f"Link: {dato5}")
                                    print("-" * 30)
                            
                        else:
                            print(f"Error al acceder al enlace: {link}, Código de estado: {response_link.status_code}")
            
                else:
                    print(f"Error al acceder a la página base. Código de estado: {response.status_code}")

    return Nombres, Epigrafes, Fecha_publicacion, Fecha_vigor, Link2, Tipos, Entidades


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
    base_url = "https://www.cali.gov.co/aplicaciones/boletin_publico/"
    data = scrapping(base_url)
    if data:
        response = insert_new_records(df=data)
        print(f"Datos insertados en Supabase: {response}")
    return {"statusCode": 200, "body": json.dumps("Scraping completado y datos cargados")}
