import requests
import pandas as pd
from datetime import datetime

# URL y parámetros base
api_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"
params_base = {
    "juwpfisadmin": "false",
    "action": "wpfd",
    "task": "files.display",
    "view": "files",
    "id": "78",
    "rootcat": "78",
    "orderCol": "title",
    "orderDir": "desc"
}

data_list = []
pagina = 1
while True:
    params = params_base.copy()
    params['page'] = pagina
    response = requests.get(api_url, params=params,verify=False)
    data = response.json()

    # Verificar si hay archivos para procesar
    if not data['files']:
        break  # No más archivos, salir del bucle

    # Procesar cada archivo en la respuesta JSON
    for item in data['files']:
        title = item['post_title']
        external_link = item['linkdownload']
        update_at = item['modified']  # Fecha de modificación como fecha de actualización
        created_at = item['created']  # Fecha de creación

        # Crear un diccionario con los datos y los valores constantes
        data_dict = {
            "title": title,
            "summary": title,
            "update_at": update_at,
            "external_link": external_link,
            "entity": "Alcaldía del Distrito Nacional",
            "created_at": created_at,
            "classification_id": 15,
            "rtype_id": 23,
            "gtype": "link",
            "is_active": True
        }
        data_list.append(data_dict)

    pagina += 1  # Incrementar el número de página para la siguiente solicitud

# Crear un DataFrame con los datos
df = pd.DataFrame(data_list)

# Mostrar el DataFrame
print(df)
