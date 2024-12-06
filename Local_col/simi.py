import requests
from bs4 import BeautifulSoup
import pandas as pd

page = requests.get('https://simi.concejodemedellin.gov.co/simi3/invitados/proyectos/acuerdos.xhtml')
soup = BeautifulSoup(page.text, 'html.parser')

# Buscar la tabla
table = soup.find("table")
#Creacion de listas
Nombres=[]
Fechas=[]
Epigrafe=[]
Tipo=[]
botones=[]
tbody = table.find("tbody")
#Recorrer y añadir celdas a cada lista
if tbody:
    for row_index, row in enumerate(tbody.find_all("tr"), start=1):
        for col_index, cell in enumerate(row.find_all("td"), start=1):
            cell_content = cell.get_text(strip=True)    
            if col_index == 1: Nombre
                Nombres.append(cell_content)
            elif col_index == 2:Fecha
                Fechas.append(cell_content)
            elif col_index == 3:Epigrafe
                Epigrafe.append(cell_content)
            elif col_index == 4: Tipo
                Tipo.append(cell_content)
            elif col_index == 5: botones
                botones.append(cell_content)
#Imprimir cada una de las listas
print(Nombres)
print(Fechas)
print(Epigrafe)
print(Tipo)
print(botones)




    

    
