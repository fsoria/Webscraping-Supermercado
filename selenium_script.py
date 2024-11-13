from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

def scrape_webpage(url):
    # Configuración del servicio para el controlador de Chrome
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    driver.get(url)

    # Configuración de espera
    wait = WebDriverWait(driver, 15)
    product_locator = (By.CSS_SELECTOR, ".vtex-product-summary-2-x-productBrand.vtex-product-summary-2-x-brandName.t-body")

    max_attempts = 30
    attempts = 0
    last_count = 0

    while attempts < max_attempts:
        # Intentar hacer clic en el botón "Cargar más" si está presente
        try:
            load_more_button = wait.until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".vtex-button__label.flex.items-center.justify-center.h-100.ph5"))
            )
            load_more_button.click()
            time.sleep(3)  # Espera después de hacer clic en el botón
        except:
            # Si no hay botón de "Cargar más", continúa haciendo scroll
            pass

        # Scroll hasta el final de la página para activar carga dinámica
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # Espera para permitir que los productos carguen
        time.sleep(5)

        # Contar los productos actualmente visibles en la página
        products = driver.find_elements(*product_locator)
        current_count = len(products)

        # Si no se agregan más productos, incrementa el contador de intentos
        if current_count == last_count:
            attempts += 1
        else:
            # Restablecer intentos si se detectan más productos
            attempts = 0
            last_count = current_count

    # Obtener el contenido de la página después de todos los intentos de carga
    page_source = driver.page_source
    driver.quit()
    return page_source 
