import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin  # Para combinar URLs
import re

# URL base de la página
base_url = "https://www.cali.gov.co/aplicaciones/boletin_publico/"
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
# Imprimir los datos procesados
print("\nNombres extraídos:", Nombres)
print("Links extraídos:", Link2)
print("Tipos:",Tipos)
print("Entidades:",Entidades)
print("Epígrafes extraídos:", Epigrafes)
print("Fechas de publicación:", Fecha_publicacion)
print("Fechas de vigor:", Fecha_vigor)
