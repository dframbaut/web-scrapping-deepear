import requests
from bs4 import BeautifulSoup
import csv

def scrape_pdfs(base_url, start_year, end_year):
    all_data = []

    for year in range(start_year, end_year + 1):
        url = f"{base_url}/acuerdos-{year}/"
        print(f"Scraping {url}...")

        # Realizar una solicitud a la página del año
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error al acceder a {url}. Código de estado: {response.status_code}")
            continue

        soup = BeautifulSoup(response.content, 'html.parser')

        # Encontrar los elementos <p> con los PDFs y descripciones
        pdf_sections = soup.find_all('p')  # Buscar todos los elementos <p>
        for pdf_section in pdf_sections:
            # Intentar extraer el enlace al PDF
            link_tag = pdf_section.find('a', href=True)
            if link_tag:
                pdf_link = link_tag['href']
                text = pdf_section.get_text(strip=True)

                # Procesar el texto para separar título y epígrafe
                if '¨' in text:
                    parts = text.split('¨')
                    titulo = parts[0].strip()
                    epigrafe = parts[1].strip() if len(parts) > 1 else ""
                else:
                    titulo = text
                    epigrafe = ""

                all_data.append({
                    'year': year,
                    'pdf_link': pdf_link,
                    'titulo': titulo,
                    'epigrafe': epigrafe
                })

    return all_data

# Configuración de URL y rango de años
base_url = "https://www.concejodebarranquilla.gov.co"
start_year = 2008
end_year = 2024

# Llamada a la función de scraping
data = scrape_pdfs(base_url, start_year, end_year)

# Guardar los resultados en un archivo CSV
output_file = 'acuerdos_barranquilla.csv'
with open(output_file, 'w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=['year', 'pdf_link', 'titulo', 'epigrafe'])
    writer.writeheader()
    writer.writerows(data)

print(f"Scraping completado. Datos guardados en {output_file}")