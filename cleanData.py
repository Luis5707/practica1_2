from difflib import get_close_matches
import pandas as pd
import os
from datetime import datetime
import unicodedata
import re

from enumIincidencias import IncidenciaUsuario

#TODO Variables para las clases de cada base

CLASES_AREA = ["ID", "LATITUD", "LONGITUD", "BARRIO", "DISTRITO", "ESTADO", "FECHA_INSTALACION", "TOTAL_ELEM"]
CLASE_SATISFACCION = ["ID", "FECHA","PUNTUACION_ACCESIBILIDAD", "PUNTUACION_CALIDAD", "COMENTARIOS"]
CLASE_USUARIO = ["NIF", "NOMBRE", "EMAIL", "TELEFONO"]
CLASE_INDICENDIA_USUARIO = ["ID", "TIPO_INCIDENCIA", "FECHA_REPORTE", "ESTADO"] #TODO FALTA TIEMPO RESOLUCION



class CSVProcessor:
    def __init__(self, directorio):
        self.directorio = directorio
        self.enumIndicencias = IncidenciaUsuario  # Instancia de EnumValuesIncidencias
        self.ruta_archivo = None
    
    def cargar_y_reemplazar_csv(self, ruta_archivo):
        self.ruta_archivo = ruta_archivo        # Actualizarlo segun el archivo
        df = pd.read_csv(ruta_archivo)

        for columna in df.columns:
            df[columna] = df[columna].map(
                lambda x: 'None' if pd.isna(x) or str(x).strip() == '' else self.corregir_tipografia(x))


        #if any(col.lower() == 'telefono' for col in df.columns):
        #    df['teléfono'] = df['teléfono'].apply(self.formatear_telefono(x))

        for columna in df.columns:
            if 'fecha' in columna.lower():
                try:
                    df[columna] = df[columna].map(lambda x: self.corregir_fecha(x))
                    print(f"Columna '{columna}' estandarizada al formato de fecha ISO.")
                except Exception as e:
                    print(f"Error al convertir la columna '{columna}': {e}")

            elif 'telefono' in columna.lower():
                df[columna] = df[columna].map(lambda x: self.formatear_telefono(x))

            elif 'cod_postal' in columna.lower():
                df[columna] = df[columna].map(lambda x: self.corregir_cod_postal(x))

        # Quitar los duplicados
        if 'ID' in df.columns:
            df = df.drop_duplicates(subset='ID', keep='first')
            print("Duplicados en la columna 'ID' eliminados.")

        df.to_csv(ruta_archivo, index=False)
        print(
            f"Archivo '{ruta_archivo}' procesado y guardado con valores nulos, fechas estandarizadas y errores tipográficos corregidos.")

    def corregir_fecha(self, fecha):
        if fecha == 'None':
            return 'None'

        try:
            return pd.to_datetime(fecha, errors='raise').isoformat()
        except:
            pass

        try:
            return pd.to_datetime(fecha, format='%d/%m/%Y', errors='raise').isoformat()
        except:
            pass

        return 'None'

    def formatear_telefono(self, numero):
        # Eliminar cualquier carácter no numérico
        numero = re.sub(r'\D', '', str(numero))

        # Verificar que el número tiene al menos 9 o 10 dígitos
        if len(numero):
            return f'+{numero[:2]} {numero[2:5]} {numero[5:8]} {numero[8:]}'
        else:
            # Si el número no tiene el formato adecuado, retornarlo tal cual
            return numero

    def corregir_cod_postal(self, texto):
        if texto != None:
            return str(texto)[:-2]
        else:
            return texto

    def corregir_tipografia(self, texto):

        #TODO ejeplo para un tipo de archivo
        try:
            if isinstance(texto, str):
                texto = texto.upper()       # Convertir a mayusculas
                texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
                return texto
        except Exception as e:
            print(f"Error al corregir tipografía: {e}")
            return texto
        return texto

    def corregir_enum_incidencias(self, valor, valores_permitidos):

        if valor in valores_permitidos:
            return valor
        else:
            coincidencias = get_close_matches(valor, valores_permitidos, n=1, cutoff=0.6)
            return coincidencias[0] if coincidencias else 'None'

    def procesar_archivos_csv(self):
        for archivo in os.listdir(self.directorio):
            if archivo.endswith('.csv'):
                ruta_archivo = os.path.join(self.directorio, archivo)
                self.cargar_y_reemplazar_csv(ruta_archivo)


# Directorio donde están los archivos .csv
directorio = '/home/luis/Escritorio/practica1_2/Datasets Práctica 1.2-20241015'

# Crear una instancia de la clase CSVProcessor
procesador_csv = CSVProcessor(directorio)

# Procesar todos los archivos .csv en el directorio
procesador_csv.procesar_archivos_csv()
