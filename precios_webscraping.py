import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup as bs
import psycopg2
import time
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
    chrome_options.add_argument("--headless")  # Ejecuta en modo sin cabeza (sin abrir ventana del navegador)
    driver_path = 'C:/Users/ferso/OneDrive/Escritorio/Proyectos/Precios supermercados/chromedriver.exe' 
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--log-level=3")  # Solo errores

    
    driver.get(url)
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    # Realiza scroll hasta que ya no haya más carga dinámica
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

# Lista de categorías con sus respectivas URLs y clases de HTML
categorias = [
    {'url': 'https://diaonline.supermercadosdia.com.ar/leche?_q=leche&map=ft', 
     'nombre_clase': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',
     'precio_clase': 'vtex-product-price-1-x-sellingPriceValue'},
    
    {'url': 'https://diaonline.supermercadosdia.com.ar/yerba?_q=yerba&map=ft', 
     'nombre_clase': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',  
     'precio_clase': 'vtex-product-price-1-x-sellingPrice'},  
    
    {'url': 'https://diaonline.supermercadosdia.com.ar/arroz?_q=arroz&map=ft', 
     'nombre_clase': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',  
     'precio_clase': 'vtex-product-price-1-x-sellingPrice vtex-product-price-1-x-sellingPrice--hasListPrice'}, 

    {'url': 'https://diaonline.supermercadosdia.com.ar/panales?_q=panales&map=ft', 
     'nombre_clase': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',  
     'precio_clase': 'vtex-product-price-1-x-sellingPrice'},  
]

productos_combinados = []
palabras_a_omitir = ['crema', 'postre', 'condensada', 'fermentada', 'chocolatada', 'toallita', 'ropa', 'membrillo' ]
fecha_scraping = datetime.now().strftime('%Y-%m-%d')

for categoria in categorias:
    response = scrape_webpage(categoria['url'])
    soup = bs(response, 'html.parser')

    listado_nombres = soup.find_all('span', class_=categoria['nombre_clase'])
    listado_precios = soup.find_all('span', class_=categoria['precio_clase'])

    productos = [nombre.text.strip() for nombre in listado_nombres]
    productos_precio = [precio.text.strip() for precio in listado_precios if precio.find_previous('span', class_=categoria['nombre_clase'])]

    for nombre, precio in zip(productos, productos_precio):
        if not any(palabra in nombre.lower() for palabra in palabras_a_omitir):
            productos_combinados.append({'Nombre': nombre, 'Precio': precio, 'Fecha_Scraping': fecha_scraping})

df_combinado = pd.DataFrame(productos_combinados)

def save_to_postgresql(dataframe, db_config):
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS productos (
            id SERIAL PRIMARY KEY,
            nombre VARCHAR(255),
            precio VARCHAR(50),
            fecha_scraping DATE
        );
        """
        cursor.execute(create_table_query)
        
        for _, row in dataframe.iterrows():
            insert_query = """
            INSERT INTO productos (nombre, precio, fecha_scraping) 
            VALUES (%s, %s, %s);
            """
            cursor.execute(insert_query, (row['Nombre'], row['Precio'], row['Fecha_Scraping']))
        
        conn.commit()
        print("Datos guardados en PostgreSQL correctamente.")
        
    except Exception as e:
        print(f"Error al guardar datos en PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()

save_to_postgresql(df_combinado, db_config)
