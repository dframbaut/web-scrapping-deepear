from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pandas as pd

# Configurar el navegador
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Modo sin interfaz gráfica
driver = webdriver.Chrome(options=options)

# URL base
url = "https://simi.concejodemedellin.gov.co/simi3/invitados/proyectos/acuerdos.xhtml"
driver.get(url)

# Lista para almacenar los datos
data = []

# Esperar la carga inicial
time.sleep(3)

# Procesar todas las páginas
while True:
    print("Procesando página...")

    # Encontrar la tabla y extraer filas
    table = driver.find_element(By.TAG_NAME, 'table')
    rows = table.find_elements(By.TAG_NAME, 'tr')

    # Extraer datos de todas las filas (omitiendo el encabezado)
    for row in rows[1:]:  # Omitir el encabezado
        cells = row.find_elements(By.TAG_NAME, 'td')
        if len(cells) > 0:
            data.append({
                "No. Acuerdo": cells[0].text.strip(),
                "No. Proyecto de Acuerdo": cells[1].text.strip(),
                "Título": cells[2].text.strip(),
                "Estado": cells[3].text.strip()
            })

    # Intentar ir a la siguiente página
    try:
        next_button = driver.find_element(By.CSS_SELECTOR, '.ui-paginator-next')  # Selector del botón "Siguiente"
        if 'ui-state-disabled' in next_button.get_attribute('class'):  # Verificar si el botón está deshabilitado
            print("Última página alcanzada.")
            break
        next_button.click()
        time.sleep(3)  # Esperar carga
    except Exception as e:
        print("Error al intentar pasar a la siguiente página:", e)
        break

# Convertir los datos a DataFrame
df = pd.DataFrame(data)
# Guardar los datos en un archivo CSV
df.to_csv("datos_acuerdos_completos.csv", index=False)

# Cerrar el navegador
driver.quit()