import requests
import pandas as pd
import unicodedata
import re

# URL base de la API
api_url = "https://adn.gob.do/transparencia/wp-admin/admin-ajax.php"

# Parámetros iniciales de la solicitud
params = {
    "juwpfisadmin": "false",
    "action": "wpfd",
    "task": "files.display",
    "view": "files",
    "id": 78,
    "rootcat": 78,
    "page": 1,
    "orderCol": "title",
    "orderDir": "desc"
}

# Función para normalizar texto (remover tildes y convertir a minúsculas)
def normalize_text(text):
    text = unicodedata.normalize('NFD', text)
    text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')
    return text.lower()

# Función para procesar títulos y extraer nombre y epígrafe
def process_title(title):
    """
    Extrae 'Decreto #XXX #XX' del título, incluso si la palabra 'Decreto' está pegada.
    Devuelve el nombre y el epígrafe.
    """
    title = normalize_text(title).replace("-", " ")  # Normalizar texto y quitar guiones

    # Buscar la palabra "decreto" seguida de un patrón numérico
    match = re.search(r"(decreto)?\s*(\d{1,4}\s?\d{2})", title)
    if match:
        numero = match.group(2).replace(" ", " ")  # Extrae el número
        nombre = f"Decreto {numero}"
        epigrafe = title.replace(match.group(0), "").strip(",.- ")
        return nombre, epigrafe.capitalize()

    # Si no se encuentra 'decreto', buscar solo números
    fallback_match = re.search(r"(\d{1,4}\s?\d{2})", title)
    if fallback_match:
        numero = fallback_match.group(1).replace(" ", " ")
        nombre = f"Decreto {numero}"
        epigrafe = title.replace(fallback_match.group(0), "").strip(",.- ")
        return nombre, epigrafe.capitalize()

    # Si no hay coincidencias
    return "Sin-nombre", title.capitalize()

# Función principal para obtener los archivos y construir el DataFrame
def fetch_files(api_url, params):
    page = 1
    all_files = []

    while True:
        print(f"Procesando página {page}...")
        params["page"] = page
        response = requests.get(api_url, params=params, verify=False)

        if response.status_code == 200:
            data = response.json()
            archivos = data.get("files", [])

            if not archivos:
                print("No hay más archivos disponibles.")
                break

            # Procesar cada archivo
            for archivo in archivos:
                titulo_completo = archivo.get("post_title", "")
                nombre, epigrafe = process_title(titulo_completo)
                link = archivo.get("linkdownload", "")
                fecha_publicacion = archivo.get("created_time", "")
                fecha_vigor = archivo.get("modified_time", "")

                # Añadir datos al listado
                all_files.append({
                    "Nombre": nombre,
                    "link_documento": link,
                    "Epigrafe": epigrafe,
                    "Tipo": "Decreto",
                    "Entidad": "Alcaldía del Distrito Nacional",
                    "Fecha_publicacion": fecha_publicacion,
                    "Fecha_vigor": fecha_vigor
                })

            page += 1  # Avanzar a la siguiente página
        else:
            print(f"Error en la solicitud: {response.status_code}")
            break

    return all_files

# Ejecutar la función y construir el DataFrame
all_files = fetch_files(api_url, params)

# Crear DataFrame y guardar en CSV
df = pd.DataFrame(all_files)
print("\nDataFrame Final:")
print(df)

# Guardar en CSV
df.to_csv("decretos_adn_normalizados.csv", index=False, encoding="utf-8")
print("Datos guardados en 'decretos_adn_normalizados.csv'.")