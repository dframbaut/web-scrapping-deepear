import requests
from bs4 import BeautifulSoup
import re

page = requests.get('https://concejodebogota.gov.co/acuerdos-y-resoluciones-2024/concejo/2024-01-24/201722.php')
soup = BeautifulSoup(page.text, 'html.parser')
div_content = soup.find('div', class_='post_content')

  # Encontrar el elemento <a> dentro de un <p>
 # Buscar todas las etiquetas <a>
for p_tag in div_content.find_all('p'):
    for a_tag in p_tag.find_all('a',string=lambda s: s and 'ACUERDO'):
        Nombre= a_tag.get_text(strip=True)  # Extraer el texto dentro del <a>
        Link_documento=a_tag.get('href')
        tipo= Nombre.split(' ')[0]
    Epigrafe=p_tag.get_text(strip=True)
    print(f"Texto encontrado: {Nombre}",f"Link:{Link_documento}",f"Epigrafe:{Epigrafe}",f"Tipo:{tipo}","Entidad: Concejo de Bogotá")

