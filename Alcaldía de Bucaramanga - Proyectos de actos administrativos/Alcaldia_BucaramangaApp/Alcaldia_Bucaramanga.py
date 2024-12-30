from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time

# Configuración del navegador
options = Options()
options.add_argument('--headless')  # Navegación sin interfaz gráfica
options.add_argument('--no-sandbox')  # Necesario para Colab
options.add_argument('--disable-dev-shm-usage')  # Evitar problemas de memoria
options.add_argument('--disable-gpu')

# Inicializar el controlador de Selenium
driver = webdriver.Chrome(options=options)

# URL base
url = "https://www.bucaramanga.gov.co/proyectos-de-normas-para-comentarios/"
driver.get(url)

# Inicializar listas
Nombres, Tipo, Mes, Fecha_inicio, Fecha_fin, Epigrafe, URLink = [], [], [], [], [], [], []

# Función para extraer datos de la tabla en la página actual
def extract_table_data():
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    table = soup.find("table", id="tablepress-517")
    if table:
        tbody = table.find("tbody")
        if tbody:
            for row in tbody.find_all("tr"):
                row_name = row_type = row_month = row_start_date = row_end_date = row_epigraph = row_link = None
                for col_index, cell in enumerate(row.find_all("td"), start=1):
                    cell_content = cell.get_text(strip=True)
                    a_tag = cell.find('a')
                    if a_tag and a_tag.get('href'):
                        row_link = a_tag.get('href')
                    if col_index == 1: row_name = cell_content
                    elif col_index == 2: row_type = cell_content
                    elif col_index == 3: row_month = cell_content
                    elif col_index == 4: row_start_date = cell_content
                    elif col_index == 5: row_end_date = cell_content
                    elif col_index == 6: row_epigraph = cell_content
                if row_name: Nombres.append(row_name)
                if row_type: Tipo.append(row_type)
                if row_month: Mes.append(row_month)
                if row_start_date: Fecha_inicio.append(row_start_date)
                if row_end_date: Fecha_fin.append(row_end_date)
                if row_epigraph: Epigrafe.append(row_epigraph)
                if row_link: URLink.append(row_link)

# Extraer datos de la primera página
extract_table_data()

# Navegar por las páginas siguientes
while True:
    try:
        next_button = driver.find_element("id", "tablepress-517_next")
        if "disabled" in next_button.get_attribute("class"):
            break  # Salir si el botón está deshabilitado
        next_button.click()
        time.sleep(2)  # Esperar a que la página se actualice
        extract_table_data()
    except Exception as e:
        print("Error al navegar:", e)
        break

# Cerrar el navegador
driver.quit()

# Mostrar los datos extraídos
print("Nombres:", Nombres)
print("Tipos:", Tipo)
print("Mes:", Mes)
print("Fechas de inicio:", Fecha_inicio)
print("Fechas de fin:", Fecha_fin)
print("Epígrafes:", Epigrafe)
print("Links:", URLink)
