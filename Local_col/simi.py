import requests
from bs4 import BeautifulSoup
import pandas as pd

page = requests.get('https://simi.concejodemedellin.gov.co/simi3/invitados/proyectos/acuerdos.xhtml')
soup = BeautifulSoup(page.text, 'html.parser')

# Buscar la tabla
table = soup.find("table")
#Creacion de listas para guardar datos de cada celda
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
            #Cada columna debe ser un dato diferente 
            if col_index == 1:  #N° de los acuerdos
                Nombres.append(cell_content)  # Convertir en mayúsculas
            elif col_index == 2:  #N° proyecto de acuerdo
                Fechas.append(cell_content)
            elif col_index == 3:  #Epigrafe
                Epigrafe.append(cell_content)
            elif col_index == 4:  #Tipo o estado
                Tipo.append(cell_content) 
            elif col_index == 5:  #botones ¿cual es el link?
                botones.append(cell_content)  # Capitalizar
#Se imprime cada una de las listas creadas
print(Nombres)
print(Fechas)
print(Epigrafe)
print(Tipo)
print(botones)




    

    
