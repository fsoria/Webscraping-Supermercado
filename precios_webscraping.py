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
            supermercado VARCHAR(255),
            fecha_scraping DATE
        );
        """
        cursor.execute(create_table_query)

        # Convertir el DataFrame a una lista de tuplas
        data_tuples = [tuple(row) for row in dataframe.to_numpy()]

        # Insertar todos los datos de una vez
        insert_query = """
        INSERT INTO productos (nombre, precio, supermercado, fecha_scraping) 
        VALUES (%s, %s, %s, %s);
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
    # Clases CSS para los nombres de los productos (común para todos los supermercados)
    clase_nombre = 'vtex-product-summary-2-x-productBrand vtex-product-summary-2-x-brandName t-body'

    # Clases CSS para los precios de cada supermercado
    clase_precio_dia = 'diaio-store-5-x-sellingPriceValue'  # Precio en Dia
    clase_precio_carrefour = 'valtech-carrefourar-product-price-0-x-currencyContainer'  # Precio en Carrefour
    clase_precio_vea = 'veaargentina-store-theme-1dCOMij_MzTzZOCohX1K7w'  # Precio en Vea

    # URLs de categorías y nombres de supermercados
    urls = [
        # Super1: Dia
        {'url': 'https://diaonline.supermercadosdia.com.ar/leche?_q=leche&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/yerba?_q=yerba&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/arroz?_q=arroz&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/panales?_q=panales&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/aceite?_q=aceite&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/papel%20higienico?_q=papel%20higienico&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},
        {'url': 'https://diaonline.supermercadosdia.com.ar/harina?_q=harina&map=ft', 'precio': clase_precio_dia, 'supermercado': 'Dia'},

        # Super2: Carrefour
        {'url': 'https://www.carrefour.com.ar/Lacteos-y-productos-frescos/Leches?order=', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/Desayuno-y-merienda/Yerba?order=', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/arroz?_q=arroz&map=ft', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/Mundo-Bebe/Panales?order=', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/almacen/aceites-y-vinagres?initialMap=c,c&initialQuery=almacen/aceites-y-vinagres&map=category-1,category-2,category-3,category-3,category-3,category-3&order=&query=/almacen/aceites-y-vinagres/aceites-comunes/aceites-de-oliva/aceites-en-aerosol/aceites-especiales&searchState', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/Limpieza/Papeles-higienicos?order=', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},
        {'url': 'https://www.carrefour.com.ar/Almacen/Harinas?order=', 'precio': clase_precio_carrefour, 'supermercado': 'Carrefour'},

        # Super3: Vea
        {'url': 'https://www.vea.com.ar/leche?_q=leche&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/yerba?_q=yerba&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/arroz?_q=arroz&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/pa%C3%B1ales?_q=pa%C3%B1ales&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/aceite?_q=aceite&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/papel%20higienico?_q=papel%20higienico&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'},
        {'url': 'https://www.vea.com.ar/harina?_q=harina&map=ft', 'precio': clase_precio_vea, 'supermercado': 'Vea'}
    ]


    categorias = [{'url': item['url'], 'nombre': clase_nombre, 'precio': item['precio'], 'supermercado': item['supermercado']} for item in urls]

    productos_combinados = []
    palabras_a_omitir = ['crema', 'postre', 'condensada', 'fermentada', 'chocolatada', 'toallita', 'ropa', 'membrillo']
    fecha_scraping = datetime.now().strftime('%Y-%m-%d')

    for categoria in categorias:
        try:
            response = scrape_webpage(categoria['url'])
            soup = bs(response, 'html.parser')

            # Extraer nombres de productos
            listado_nombres = soup.find_all('span', class_=categoria['nombre'])
            productos = [nombre.text.strip() for nombre in listado_nombres]

            # Extraer precios de productos
            listado_precios = soup.find_all('span', class_=categoria['precio'])
            productos_precio = [precio.text.strip() for precio in listado_precios]

            # Combinar nombres y precios
            for nombre, precio in zip(productos, productos_precio):
                if not any(palabra in nombre.lower() for palabra in palabras_a_omitir):
                    productos_combinados.append({'Nombre': nombre, 'precio': precio, 'Supermercado': categoria['supermercado'], 'Fecha_Scraping': fecha_scraping})

        except Exception as e:
            print(f"Error al scrapear la categoría {categoria['url']}: {e}")

    # Crear DataFrame y limpiar precios
    df_combinado = pd.DataFrame(productos_combinados)
    df_combinado['precio'] = df_combinado['precio'].replace('[\$,]', '', regex=True).astype(float)

    # Guardar en MySQL
    save_to_mysql(df_combinado, db_config)

if __name__ == "__main__":
    main()
