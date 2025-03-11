from flask import Flask, render_template, request
import mysql.connector
from precios_webscraping import db_config  # Importar la configuración de la base de datos

app = Flask(__name__)

# Función para conectar a la base de datos
def conectar_db():
    return mysql.connector.connect(**db_config)

@app.route("/")
def mostrar_precios():
    try:
        db = conectar_db()
        cursor = db.cursor()

        # Obtener parámetros de búsqueda y ordenación
        nombre = request.args.get("nombre", "").strip()
        orden = request.args.get("orden", "desc")  # "asc" o "desc"
        fecha_desde = request.args.get("fecha_desde", "").strip()
        supermercado = request.args.get("supermercado", "").strip()

        # Construir la consulta SQL con filtros
        query = "SELECT * FROM productos WHERE 1=1"
        params = []

        if nombre:
            query += " AND nombre LIKE %s"
            params.append(f"%{nombre}%")
        if fecha_desde:
            query += " AND fecha_scraping >= %s"
            params.append(fecha_desde)
        if supermercado:
            query += " AND supermercado LIKE %s"
            params.append(f"%{supermercado}%")

        # Agregar ordenación por precio
        if orden in ["asc", "desc"]:
            query += f" ORDER BY precio {orden.upper()}"

        cursor.execute(query, tuple(params))
        datos = cursor.fetchall()

        cursor.close()
        db.close()

        return render_template("index.html", precios=datos, nombre=nombre, orden=orden, fecha_desde=fecha_desde, supermercado=supermercado)

    except mysql.connector.Error as err:
        return f"Error al conectar con la base de datos: {err}"

if __name__ == "__main__":
    app.run(debug=True)
