import mysql.connector
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde .env
load_dotenv()

# Configuración de conexión a la base de datos MySQL
db_config = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'port': os.getenv('DB_PORT')
}

def conectar_a_mysql():
    """
    Conecta a la base de datos MySQL.
    """
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except mysql.connector.Error as err:
        print(f"Error al conectar a MySQL: {err}")
        return None

def consultar_precios_por_supermercado():
    """
    Consulta los precios de los productos, separados por supermercado.
    """
    conn = conectar_a_mysql()
    if conn is None:
        return

    try:
        cursor = conn.cursor()

        # Consulta SQL para obtener precios por supermercado
        query = """
        SELECT
            nombre,
            MAX(CASE WHEN supermercado = 'Dia' THEN CONCAT('$', FORMAT(precio, 3)) END) AS precio_dia,
            MAX(CASE WHEN supermercado = 'Carrefour' THEN CONCAT('$', FORMAT(precio, 3)) END) AS precio_carrefour,
            MAX(CASE WHEN supermercado = 'Vea' THEN CONCAT('$', FORMAT(precio, 3)) END) AS precio_vea
        FROM
            productos
        GROUP BY
            nombre;
        """
        cursor.execute(query)

        # Obtener los resultados
        resultados = cursor.fetchall()

        # Imprimir los resultados
        print("Precios por supermercado:")
        for row in resultados:
            print(f"Producto: {row[0]}, Precio Dia: {row[1]}, Precio Carrefour: {row[2]}, Precio Vea: {row[3]}")

    except mysql.connector.Error as err:
        print(f"Error al ejecutar la consulta: {err}")
    finally:
        cursor.close()
        conn.close()

def consultar_productos_mas_caros():
    """
    Consulta los productos más caros por supermercado.
    """
    conn = conectar_a_mysql()
    if conn is None:
        return

    try:
        cursor = conn.cursor()

        # Consulta SQL para obtener los productos más caros por supermercado
        query = """
        SELECT
            supermercado,
            nombre,
            CONCAT('$', FORMAT(MAX(precio), 3)) AS precio_maximo
        FROM
            productos
        GROUP BY
            supermercado, nombre
        ORDER BY
            precio_maximo DESC;
        """
        cursor.execute(query)

        # Obtener los resultados
        resultados = cursor.fetchall()

        # Imprimir los resultados
        print("Productos más caros por supermercado:")
        for row in resultados:
            print(f"Supermercado: {row[0]}, Producto: {row[1]}, Precio: {row[2]}")

    except mysql.connector.Error as err:
        print(f"Error al ejecutar la consulta: {err}")
    finally:
        cursor.close()
        conn.close()

def consultar_yerbas_carrefour():
    """
    Consulta los precios de las yerbas en el supermercado Carrefour.
    """
    conn = conectar_a_mysql()
    if conn is None:
        return

    try:
        cursor = conn.cursor()

        # Consulta SQL para obtener las yerbas en Carrefour
        query = """
        SELECT
            nombre,
            CONCAT('$', FORMAT(precio, 3)) AS precio
        FROM
            productos
        WHERE
            nombre LIKE '%yerba%' AND supermercado = 'Carrefour';
        """
        cursor.execute(query)

        # Obtener los resultados
        resultados = cursor.fetchall()

        # Imprimir los resultados
        if resultados:
            print("Yerbas en Carrefour:")
            for row in resultados:
                print(f"Producto: {row[0]}, Precio: {row[1]}")
        else:
            print("No se encontraron yerbas en Carrefour.")

    except mysql.connector.Error as err:
        print(f"Error al ejecutar la consulta: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Ejecutar las consultas
    consultar_precios_por_supermercado()
    consultar_productos_mas_caros()
    consultar_yerbas_carrefour()  # Nueva función