import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup as bs
import mysql.connector  
import time
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde .env
load_dotenv(".env")

# Configuración de conexión a la base de datos MySQL
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT')  
}

def scrape_webpage(url):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-logging")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--log-level=3")

    # Usar webdriver_manager para manejar ChromeDriver automáticamente
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

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


def save_to_mysql(dataframe, db_config):
    conn = None
    cursor = None
    try:
        # Establecer la conexión a MySQL
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Crear la tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS productos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(255),
            precio DECIMAL(10, 2),
            fecha_scraping DATE
        );
        """
        cursor.execute(create_table_query)

        # Convertir el DataFrame a una lista de tuplas
        data_tuples = [tuple(row) for row in dataframe.to_numpy()]

        # Insertar todos los datos de una vez
        insert_query = """
        INSERT INTO productos (nombre, precio, fecha_scraping) 
        VALUES (%s, %s, %s);
        """
        cursor.executemany(insert_query, data_tuples)

        conn.commit()
        print("Datos guardados en MySQL correctamente.")

    except mysql.connector.Error as err:
        print(f"Error al guardar datos en MySQL: {err}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    # Lista de categorías con sus respectivas URLs y clases de HTML
    categorias = [
        {'url': 'https://diaonline.supermercadosdia.com.ar/leche?_q=leche&map=ft',
         'nombre': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',
         'precio': 'diaio-store-5-x-sellingPriceValue'},

        {'url': 'https://diaonline.supermercadosdia.com.ar/yerba?_q=yerba&map=ft',
         'nombre': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',
         'precio': 'diaio-store-5-x-sellingPriceValue'},

        {'url': 'https://diaonline.supermercadosdia.com.ar/arroz?_q=arroz&map=ft',
         'nombre': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',
         'precio': 'diaio-store-5-x-sellingPriceValue'},

        {'url': 'https://diaonline.supermercadosdia.com.ar/panales?_q=panales&map=ft',
         'nombre': 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body',
         'precio': 'diaio-store-5-x-sellingPriceValue'},
    ]

    productos_combinados = []
    palabras_a_omitir = ['crema', 'postre', 'condensada', 'fermentada', 'chocolatada', 'toallita', 'ropa', 'membrillo']
    fecha_scraping = datetime.now().strftime('%Y-%m-%d')

    for categoria in categorias:
        try:
            response = scrape_webpage(categoria['url'])
            soup = bs(response, 'html.parser')

            listado_nombres = soup.find_all('span', class_=categoria['nombre'])
            listado_precios = soup.find_all('span', class_=categoria['precio'])

            productos = [nombre.text.strip() for nombre in listado_nombres]
            productos_precio = [precio.text.strip() for precio in listado_precios if precio.find_previous('span', class_=categoria['nombre'])]

            for nombre, precio in zip(productos, productos_precio):
                if not any(palabra in nombre.lower() for palabra in palabras_a_omitir):
                    productos_combinados.append({'Nombre': nombre, 'precio': precio, 'Fecha_Scraping': fecha_scraping})

        except Exception as e:
            print(f"Error al scrapear la categoría {categoria['url']}: {e}")  
    
    # Crear DataFrame y limpiar precios
    df_combinado = pd.DataFrame(productos_combinados)
    print(df_combinado.columns)
    print(df_combinado.head())
    df_combinado['precio'] = df_combinado['precio'].replace('[\$,]', '', regex=True).astype(float)

    # Guardar en MySQL
    save_to_mysql(df_combinado, db_config)

if __name__ == "__main__":
    main()