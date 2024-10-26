from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def scrape_webpage(url):
    # Configuración del servicio para el controlador de Chrome
    service = Service(ChromeDriverManager().install())
    
    # Iniciar el controlador de Chrome
    driver = webdriver.Chrome(service=service)

    # Navegar a la URL
    driver.get(url)

    # Obtener el contenido de la página
    page_source = driver.page_source  # Este es el contenido HTML

    driver.quit()  # Cierra el navegador
    return page_source  # Retorna el contenido
