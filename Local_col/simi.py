from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time

# Configurar el driver de Selenium (ajusta la ruta según tu sistema)
#C:\Users\User\OneDrive\Documentos\2024\Trabajo\Local_col\chromedriver-win32
driver_path = "C:/Users/User/OneDrive/Documentos/2024/Trabajo/Local_col/chromedriver-win32/chromedriver.exe"  # Cambia esto por tu ruta al driver
url = "https://simi.concejodemedellin.gov.co/simi3/invitados/proyectos/acuerdos.xhtml"

# Inicializar el navegador
driver = webdriver.Chrome(driver_path)

# Listas para almacenar los datos extraídos
Nombres = []
Fechas = []
Epigrafe = []
Tipo = []
botones = []

try:
    # Navegar a la URL
    driver.get(url)

    # Esperar a que la página cargue completamente
    time.sleep(5)  # Ajusta según sea necesario

    # Localizar la tabla
    table = driver.find_element(By.TAG_NAME, "table")

    # Encontrar todas las filas del cuerpo de la tabla
    tbody = table.find_element(By.TAG_NAME, "tbody")
    rows = tbody.find_elements(By.TAG_NAME, "tr")

    # Procesar cada fila
    for row_index, row in enumerate(rows, start=1):
        # Encontrar todas las celdas de la fila
        cells = row.find_elements(By.TAG_NAME, "td")

        for col_index, cell in enumerate(cells, start=1):
            cell_content = cell.text.strip()  # Extraer texto de la celda
            if col_index == 1:  #Nombre
                Nombres.append(cell_content)
            elif col_index == 2:  #Fecha
                Fechas.append(cell_content)
            elif col_index == 3:  #Epigrafe
                Epigrafe.append(cell_content)
            elif col_index == 4:  #Tipo
                Tipo.append(cell_content)
            elif col_index == 5:  #Enlaces
                botones.append(cell.find_element(By.TAG_NAME, "a").get_attribute("href"))

    # Imprimir las listas de datos extraídos
    print("Nombres:", Nombres)
    print("Fechas:", Fechas)
    print("Epígrafe:", Epigrafe)
    print("Tipo:", Tipo)
    print("Botones:", botones)

finally:
    # Cerrar el navegador
    driver.quit()




    

    
