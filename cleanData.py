import pandas as pd
import os
from datetime import datetime

# Función para cargar, estandarizar fechas y reemplazar valores vacíos en un CSV
def cargar_y_reemplazar_csv(ruta_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(ruta_archivo)

    # Reemplazar valores vacíos por None en cada columna usando map
    for columna in df.columns:
        df[columna] = df[columna].map(lambda x: 'None' if pd.isna(x) or str(x).strip() == '' else x)

    # Eliminar duplicados basados en la columna 'ID', si existe
    if 'ID' in df.columns:
        df = df.drop_duplicates(subset='ID', keep='first')  # Mantener la primera instancia y eliminar el resto
        print("Duplicados en la columna 'ID' eliminados.")

    # Detectar columnas que tienen "fecha" en el nombre y estandarizar las fechas
    for columna in df.columns:
        if 'fecha' in columna.lower():  # Detecta cualquier encabezado que contenga 'fecha'
            try:
                # Convertir la columna a formato de fecha compatible con MongoDB (ISODate)
                df[columna] = df[columna].map(lambda x: corregir_fecha(x))
                print(f"Columna '{columna}' estandarizada al formato de fecha ISO.")
            except Exception as e:
                print(f"Error al convertir la columna '{columna}': {e}")

    # Guardar el archivo CSV con los cambios
    df.to_csv(ruta_archivo, index=False)
    print(f"Archivo '{ruta_archivo}' procesado y guardado con valores nulos y fechas estandarizadas.")

# Función para corregir y estandarizar formatos de fecha
def corregir_fecha(fecha):
    # Si el valor es 'None', devolver None
    if fecha == 'None':
        return 'None'

    # Intentar convertir la fecha en formato ISO automáticamente
    try:
        return pd.to_datetime(fecha, errors='raise').isoformat()
    except:
        pass

    # Si la conversión automática falla, intentar detectar formatos con "/"
    try:
        return pd.to_datetime(fecha, format='%d/%m/%Y', errors='raise').isoformat()
    except:
        pass

    # Si aún así no se puede convertir, devolver None
    return 'None'

# Función para procesar todos los archivos .csv en un directorio
def procesar_archivos_csv(directorio):
    # Recorrer todos los archivos en el directorio
    for archivo in os.listdir(directorio):
        if archivo.endswith('.csv'):
            ruta_archivo = os.path.join(directorio, archivo)
            cargar_y_reemplazar_csv(ruta_archivo)

# Directorio donde están los archivos .csv
directorio = '/home/luis/Escritorio/practica1_2/Datasets Práctica 1.2-20241012'  # Cambia esta ruta por la del directorio que contiene los archivos

# Procesar todos los archivos .csv en el directorio
procesar_archivos_csv(directorio)
