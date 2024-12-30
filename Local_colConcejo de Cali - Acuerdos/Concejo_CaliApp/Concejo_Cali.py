import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin  # Para combinar URLs
import re

# URL base de la página
base_url = "https://www.concejodecali.gov.co/documentos/5033/acuerdos-2024/"

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

# Imprimir los resultados finales
print("Nombres:", Nombres)
print("Epígrafes:", Epigrafes)
print("Fechas de publicación:", Fecha_publicacion)
print("Fechas de vigencia:", Fecha_vigor)
print("Enlaces:", Link)
