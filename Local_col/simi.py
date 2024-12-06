import requests
from bs4 import BeautifulSoup
import pandas as pd

page = requests.get('https://simi.concejodemedellin.gov.co/simi3/invitados/proyectos/acuerdos.xhtml')
soup = BeautifulSoup(page.text, 'html.parser')

# Buscar la tabla
table = soup.find("table")

Nombres=[]
Fechas=[]
Epigrafe=[]
Tipo=[]
botones=[]
tbody = table.find("tbody")
if tbody:
    for row_index, row in enumerate(tbody.find_all("tr"), start=1):
        for col_index, cell in enumerate(row.find_all("td"), start=1):
            cell_content = cell.get_text(strip=True)    
            # Lógica personalizada para cada columna
            if col_index == 1:  # Primera columna (por ejemplo, nombres)
                Nombres.append(cell_content)  # Convertir en mayúsculas
            elif col_index == 2:  # Segunda columna (por ejemplo, edades)
                Fechas.append(cell_content)
            elif col_index == 3:  # Tercera columna (por ejemplo, ciudades)
                Epigrafe.append(cell_content)
            elif col_index == 4:  # Tercera columna (por ejemplo, ciudades)
                Tipo.append(cell_content)  # Capitalizar
            elif col_index == 5:  # Tercera columna (por ejemplo, ciudades)
                botones.append(cell_content)  # Capitalizar

print(Nombres)
print(Fechas)
print(Epigrafe)
print(Tipo)
print(botones)




    

    