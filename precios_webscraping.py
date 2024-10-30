import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import time
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde .env
load_dotenv()

# Configuración de conexión a la base de datos PostgreSQL en RDS
db_config = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

def scrape_webpage(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver_path = 'C:/Users/ferso/OneDrive/Escritorio/Proyectos/Precios supermercados/chromedriver.exe'
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    
    page_source = driver.page_source
    driver.quit()
    return page_source

url = 'https://diaonline.supermercadosdia.com.ar/leche?_q=leche&map=ft'
response = scrape_webpage(url)
soup = bs(response, 'html.parser')

productos = []
productos_precio = []

listado_nombres = soup.find_all('span', class_='vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body')
listado_precios = soup.find_all('span', class_='vtex-product-price-1-x-sellingPriceValue')

for nombre in listado_nombres:
    productos.append(nombre.text.strip())

for precio in listado_precios:
    if precio.find_previous('span', class_='vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body'):
        productos_precio.append(precio.text.strip())

productos_combinados = []
palabras_a_omitir = ['crema', 'postre', 'condensada']
fecha_scraping = datetime.now().strftime('%Y-%m-%d')

for nombre, precio in zip(productos, productos_precio):
    if not any(palabra in nombre.lower() for palabra in palabras_a_omitir):
        productos_combinados.append({'Nombre': nombre, 'Precio': precio, 'Fecha_Scraping': fecha_scraping})

df_combinado = pd.DataFrame(productos_combinados)

# Guardar datos en PostgreSQL
def save_to_postgresql(dataframe, db_config):
    try:
        # Conectar a la base de datos
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Crear la tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            precio VARCHAR(50),
            fecha_scraping DATE
        );
        """
        cursor.execute(create_table_query)
        
        # Insertar datos en la tabla
        for _, row in dataframe.iterrows():
            insert_query = """
            INSERT INTO productos (nombre, precio, fecha_scraping) 
            VALUES (%s, %s, %s);
            """
            cursor.execute(insert_query, (row['Nombre'], row['Precio'], row['Fecha_Scraping']))
        
        # Guardar cambios
        conn.commit()
        print("Datos guardados en PostgreSQL correctamente.")
        
    except Exception as e:
        print(f"Error al guardar datos en PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

# Llamada a la función para guardar datos en RDS
save_to_postgresql(df_combinado, db_config)
