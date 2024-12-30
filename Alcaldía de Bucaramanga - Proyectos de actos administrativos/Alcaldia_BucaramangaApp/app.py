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

#Scrapping a la pagina
# Configuración del navegador
options = Options()
options.add_argument('--headless')  # Navegación sin interfaz gráfica
options.add_argument('--no-sandbox')  # Necesario para Colab
options.add_argument('--disable-dev-shm-usage')  # Evitar problemas de memoria
options.add_argument('--disable-gpu')

# Inicializar el controlador de Selenium
driver = webdriver.Chrome(options=options)

# URL base
def scrape (url):
    driver.get(url)
    
    # Inicializar listas
    Nombres, Tipo, Mes, Fecha_inicio, Fecha_fin, Epigrafe, URLink = [], [], [], [], [], [], []
    
    # Función para extraer datos de la tabla en la página actual
    def extract_table_data():
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        table = soup.find("table", id="tablepress-517")
        if table:
            tbody = table.find("tbody")
            if tbody:
                for row in tbody.find_all("tr"):
                    row_name = row_type = row_month = row_start_date = row_end_date = row_epigraph = row_link = None
                    for col_index, cell in enumerate(row.find_all("td"), start=1):
                        cell_content = cell.get_text(strip=True)
                        a_tag = cell.find('a')
                        if a_tag and a_tag.get('href'):
                            row_link = a_tag.get('href')
                        if col_index == 1: row_name = cell_content
                        elif col_index == 2: row_type = cell_content
                        elif col_index == 3: row_month = cell_content
                        elif col_index == 4: row_start_date = cell_content
                        elif col_index == 5: row_end_date = cell_content
                        elif col_index == 6: row_epigraph = cell_content
                    if row_name: Nombres.append(row_name)
                    if row_type: Tipo.append(row_type)
                    if row_month: Mes.append(row_month)
                    if row_start_date: Fecha_inicio.append(row_start_date)
                    if row_end_date: Fecha_fin.append(row_end_date)
                    if row_epigraph: Epigrafe.append(row_epigraph)
                    if row_link: URLink.append(row_link)
            return Nombres, Tipo, Mes, Fecha_inicio, Fecha_fin, Epigrafe, URLink
    
    # Extraer datos de la primera página
    extract_table_data()
    
    # Navegar por las páginas siguientes
    while True:
        try:
            next_button = driver.find_element("id", "tablepress-517_next")
            if "disabled" in next_button.get_attribute("class"):
                break  # Salir si el botón está deshabilitado
            next_button.click()
            time.sleep(2)  # Esperar a que la página se actualice
            extract_table_data()
        except Exception as e:
            print("Error al navegar:", e)
            break
    
    # Cerrar el navegador
    driver.quit()

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
    url = "https://www.bucaramanga.gov.co/proyectos-de-normas-para-comentarios/"
    data = scrape(url)
    if data:
        response = insert_new_records(df=data)
        print(f"Datos insertados en Supabase: {response}")
    return {"statusCode": 200, "body": json.dumps("Scraping completado y datos cargados")}
