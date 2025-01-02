#Imports necesarios
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import json
import unicodedata
from sre_constants import NOT_LITERAL
from google.colab import files


#Extraccion principal de ids


url='https://adn.gob.do/transparencia/planificacion-y-desarrollo/'

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
}

response=requests.get(url,headers=headers,verify=False)
soup=BeautifulSoup(response.text,'html.parser')

a_tags=soup.find_all('a',class_='wpfdcategory catlink')

# Extraer el atributo "data-idcat" de las etiquetas encontradas
ids_list = [tag.get('data-idcat') for tag in a_tags]

# Imprimir los valores extraídos
print("IDCAT encontrados:", ids_list)



#Extraccion ids secundarios


# URL base
base_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"

# Parámetros base para la solicitud
base_params = {
    "juwpfisadmin": "false",
    "action": "wpfd",
    "task": "categories.display",
    "view": "categories",
    "top": "108"
}

def extraer_term_taxonomy_id(base_url, params, exclude_ids):
    """
    Extrae los valores de term_taxonomy_id para un ID específico,
    excluyendo IDs ya procesados.

    :param base_url: URL base para realizar la solicitud.
    :param params: Diccionario con los parámetros de la solicitud.
    :param exclude_ids: Conjunto de IDs que deben ser excluidos.
    :return: Lista de nuevos term_taxonomy_id encontrados.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }

    try:
        # Realizar solicitud GET
        response = requests.get(base_url, params=params, headers=headers, verify=False)
        response.raise_for_status()  # Genera excepción si el código no es 200

        # Parsear el HTML de la respuesta
        soup = BeautifulSoup(response.text, 'html.parser')

        # Convertir el contenido en texto para buscar con expresiones regulares
        page_text = soup.get_text()

        # Expresión regular para buscar valores de term_taxonomy_id
        term_taxonomy_ids = re.findall(r'"term_taxonomy_id":(\d+)', page_text)

        # Filtrar IDs excluidos
        new_ids = [id_val for id_val in term_taxonomy_ids if id_val not in exclude_ids]

        return new_ids

    except requests.exceptions.RequestException as e:
        print(f"Error durante la solicitud para ID {params['id']}: {e}")
        return []
    except Exception as e:
        print(f"Error al procesar los datos para ID {params['id']}: {e}")
        return []

# Iteración para obtener IDs adicionales
processed_ids = set(ids_list)  # Conjunto para rastrear IDs ya procesados
idcat_values2 = []  # Lista para almacenar los resultados iterativos
iteration_count = 1  # Contador de iteraciones

while True:
    print(f"\nIteración {iteration_count}")
    nuevos_ids = []

    # Iterar sobre los IDs actuales y buscar nuevos term_taxonomy_id
    for id_val in ids_list:
        params = base_params.copy()
        params["id"] = id_val

        # Extraer term_taxonomy_id excluyendo los procesados
        ids_extraidos = extraer_term_taxonomy_id(base_url, params, processed_ids)
        nuevos_ids.extend(ids_extraidos)

    # Eliminar duplicados y actualizar el conjunto de IDs procesados
    nuevos_ids = list(set(nuevos_ids))  # Eliminar duplicados
    nuevos_ids = [id_val for id_val in nuevos_ids if id_val not in processed_ids]

    if not nuevos_ids:
        print("No se encontraron nuevos IDs. Finalizando.")
        break

    # Actualizar los datos para la siguiente iteración
    idcat_values2.append(nuevos_ids)
    processed_ids.update(nuevos_ids)
    ids_list = nuevos_ids
    iteration_count += 1

# Fusionar todas las listas en una sola, eliminando duplicados
final_idcat_values = list(set([item for sublist in idcat_values2 for item in sublist] + list(processed_ids)))

# Mostrar los resultados finales
print("\nResultados finales:")
print(f"Lista combinada de IDCAT: {final_idcat_values}")

#Scrapping
# URL base de la API
api_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"

# Lista de IDs que deseas recorrer
ids = final_idcat_values

# Parámetros base de la solicitud
base_params = {
    "juwpfisadmin": "false",
    "action": "wpfd",
    "task": "files.display",
    "view": "files",
    "page": 1,
    "orderCol": "title",
    "orderDir": "desc"
}

nombres = []
link = []
fecha_pub = []
fecha_vigor = []
epigrafe = []

# Función para normalizar texto (remover tildes y convertir a minúsculas)
def normalize_text(text):
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text.lower()

# Función para procesar cada ID
def fetch_files_by_id(api_url, ids, base_params):
    all_results = []  # Lista para almacenar resultados de todas las IDs

    for current_id in ids:
        print(f"Procesando ID: {current_id}...")

        # Actualizar los parámetros con el ID actual
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
                    print(f"No hay más archivos para ID: {current_id}.")
                    break

                # Agregar los archivos procesados a la lista de resultados
                for archivo in archivos:
                    post_title = archivo.get("post_title", "Sin título")
                    normalized_title = normalize_text(post_title)
                    nombres.append(normalized_title)
                    epigrafe.append(normalized_title)
                    created_time = archivo.get("created_time", "Sin fecha")
                    fecha_pub.append(created_time)
                    modified_time = archivo.get("modified_time", "Sin fecha")
                    fecha_vigor.append(modified_time)
                    linkdownload = archivo.get("linkdownload", "Sin enlace")
                    link.append(linkdownload)

                    # Agregar solo los campos requeridos al resultado
                    all_results.append({
                        "post_title": normalized_title,
                        "created_time": created_time,
                        "modified_time": modified_time,
                        "linkdownload": linkdownload
                    })

                page += 1  # Avanzar a la siguiente página
            else:
                print(f"Error al procesar ID: {current_id}. Código de estado: {response.status_code}")
                break

    return all_results

# Ejecutar la función para procesar los IDs
resultados = fetch_files_by_id(api_url, ids, base_params)

# Mostrar los resultados obtenidos
print("Archivos obtenidos:")
print(json.dumps(resultados, indent=4))

#Dataframe
# Crear un DataFrame con los datos recopilados
data = {
    "title": nombres,
    "external_link": link,
    "created_at": fecha_pub,
    "update_at": fecha_vigor,
    'summary':epigrafe
}

df = pd.DataFrame(data)

# Agregar columnas con los valores especificados
df['Tipo'] = None
df['entity']='Alcaldia del distrito Nacional'
df['classification_id']=None
df['rtype_id']='13'
df['gtype']='link'
df['is_active']=True
columnas_ordenadas = ['title','summary','update_at','external_link','entity','created_at','classification_id','rtype_id','gtype','is_active']

df = df[columnas_ordenadas]

# Mostrar el DataFrame resultante
print(df)

df.to_csv("planificacion_y_desarrollo.csv", index=False, encoding='utf-8-sig')

print("Archivo CSV generado exitosamente.")

#Descarga CSV

from google.colab import files

# Descargar el archivo generado
files.download("planificacion_y_desarrollo.csv")
