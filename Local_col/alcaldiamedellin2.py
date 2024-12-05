import requests
from bs4 import BeautifulSoup

# URL de la página
url = "https://www.medellin.gov.co/es/secretaria-general/proyectos-normativos-agenda-regulatoria/"

# Solicitar el contenido de la página
response = requests.get(url)

# Verifica si la solicitud fue exitosa
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')

    # Encontrar todos los elementos <h3>
    #Nombre
    h3_elements = soup.find_all('h3')
    print("Contenido de los elementos <h3> que contienen 'Decreto' o 'Proyecto':")
    # Filtrar y mostrar solo los que contienen "Decreto" o "Proyecto"
    filtered_h3 = [
        h3.get_text(strip=True) for h3 in h3_elements
        if "Decreto" in h3.get_text(strip=True) or "Proyecto" in h3.get_text(strip=True)
        ]

    # Imprimir los resultados filtrados
    if filtered_h3:
        for i, h3_text in enumerate(filtered_h3, start=1):
                print(f"{i}. {h3_text}")
    else:
        print("No se encontraron elementos <h3> con 'Decreto' o 'Proyecto'.")
# Encontrar todos los links para los proyectos y decretos
a_elements = soup.find_all('a')
for i, a_tag in enumerate(a_elements, start=1):
    if a_tag.find_parent('span') is None: 
        print(f"{i}. {a_tag.get('href')}") 
#Epigrafe

p_elements=soup.find_all('p')
"""
filtered_p = [
        p.get_text(strip=True) for p in p_elements
        if "Por medio" in p.get_text(strip=True)
        ]
    # Imprimir los resultados filtrados
if filtered_p:
    for i, p_text in enumerate(filtered_p, start=1):
        print(f"{i}. {p_text}")
        print()
else:
    print("No se encontraron elementos <p> con 'Fecha'.")
"""
#Fechas
filtered_p = [
        p.get_text(strip=True) for p in p_elements
        if "Fecha:" in p.get_text(strip=True)
        ]
    # Imprimir los resultados filtrados
if filtered_p:
    for i, p_text in enumerate(filtered_p, start=1):
        print(f"{i}. {p_text}")
else:
    print("No se encontraron elementos <p> con 'Fecha'.")