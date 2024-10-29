import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import time
import boto3
from io import StringIO
from datetime import datetime

def scrape_webpage(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Ejecutar en segundo plano (opcional)
    driver_path = 'C:/Users/ferso/OneDrive/Escritorio/Proyectos/Precios supermercados/chromedriver.exe'
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")  # Obtener la altura inicial

    while True:
        # Desplazarse hacia abajo
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Esperar a que se carguen los nuevos productos
        
        # Calcular nueva altura y comparar con la altura anterior
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # Si no hay más contenido para cargar
            break
        last_height = new_height  # Actualizar la altura para la próxima iteración

    page_source = driver.page_source
    driver.quit()
    return page_source

url = 'https://diaonline.supermercadosdia.com.ar/leche?_q=leche&map=ft'
response = scrape_webpage(url)
soup = bs(response, 'html.parser')

productos = []
productos_precio = []

# Encuentra los nombres y precios de los productos
listado_nombres = soup.find_all('span', class_='vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body')
listado_precios = soup.find_all('span', class_='vtex-product-price-1-x-sellingPriceValue')

# Captura los nombres de los productos
for nombre in listado_nombres:
    productos.append(nombre.text.strip())

# Captura los precios, asegurando que coincidan con los productos
for precio in listado_precios:
    if precio.find_previous('span', class_='vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body'):
        productos_precio.append(precio.text.strip())

# Inicializar la lista combinada
productos_combinados = []

# Palabras a omitir
palabras_a_omitir = ['crema', 'postre', 'condensada']

# Fecha de scraping
fecha_scraping = datetime.now().strftime('%Y-%m-%d')

# Combina los nombres y precios en un solo listado, omitiendo productos con palabras no deseadas
for nombre, precio in zip(productos, productos_precio):
    if not any(palabra in nombre.lower() for palabra in palabras_a_omitir):
        productos_combinados.append({'Nombre': nombre, 'Precio': precio, 'Fecha_Scraping': fecha_scraping})

# Convertimos la lista combinada en un DataFrame
df_combinado = pd.DataFrame(productos_combinados)

# Configuración de S3
s3 = boto3.client('s3')
bucket_name = 'webscraping-supermercado'
# Nombre del archivo único en S3
file_key = 'datafile_unico.csv'

# Intentamos descargar el archivo existente y combinarlo con los nuevos datos
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    existing_data = pd.read_csv(response['Body'])
    # Concatenar el DataFrame existente con el nuevo
    df_combinado = pd.concat([existing_data, df_combinado], ignore_index=True)
except s3.exceptions.NoSuchKey:
    print("El archivo no existe en S3; se creará un nuevo archivo.")

# Convertimos el DataFrame actualizado a CSV en memoria
csv_buffer = StringIO()
df_combinado.to_csv(csv_buffer, index=False)

# Subir el archivo actualizado a S3
s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())
print("Datos agregados al archivo CSV único en S3.")
