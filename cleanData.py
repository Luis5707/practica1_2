from difflib import get_close_matches
import pandas as pd
import os
from datetime import datetime
import unicodedata
import re


#TODO Variables para las clases de cada base

#CLASES_AREA = ["ID", "LATITUD", "LONGITUD", "BARRIO", "DISTRITO", "ESTADO", "FECHA_INSTALACION", "TOTAL_ELEM"]
#CLASE_SATISFACCION = ["ID", "FECHA","PUNTUACION_ACCESIBILIDAD", "PUNTUACION_CALIDAD", "COMENTARIOS"]
#CLASE_USUARIO = ["NIF", "NOMBRE", "EMAIL", "TELEFONO"]
#CLASE_INCICENDIA_USUARIO = ["ID", "TIPO_INCIDENCIA", "FECHA_REPORTE", "ESTADO"] # TODO FALTA TIEMPO RESOLUCION
#CLASE_INCIDENCIA_SEGURIDAD = ["ID", "FECHA_REPORTE", "TIPO_INCIDENTE", "GRAVEDAD"]
#CLASE_JUEGO = ["ID","DESC_CLASIFICACION", "MODELO", "ESTADO", "ACCESIBLE", "FECHA_INSTALACION", "TIPO_JUEGO", "FECHA_INTERVENCION"]# TODO desgasteAcumulado, indicadorExposicion no sabemos-- suponemos ultimaFechaMantenimiento de Mantenimiento
#CLASE_MANTENIMIENTO = ["ID", "FECHA_INTERVENCION", "TIPO_INTERVENCION", "ESTADO_PREVIO", "ESTADO_POSTERIOR"]
##CLASE_REGISTRO_CLIMA = [""] TODO -- No sabemos que poner aqui

NOMBRES_ARCHIVOS = {"AreasSucio.csv":
                        ["ID", "COORDENADAS", "BARRIO", "DISTRITO", "ESTADO", "FECHA_INSTALACION",
                                      "TOTAL_ELEM"],
                    "EncuestasSatisfaccionSucio.csv":[
                        "ID", "FECHA","PUNTUACION_ACCESIBILIDAD", "PUNTUACION_CALIDAD", "COMENTARIOS"],
                    "IncidenciasUsuariosSucio.csv": ["ID", "TIPO_INCIDENCIA", "FECHA_REPORTE", "ESTADO"],
                    "IncidentesSeguridadSucio.csv": ["ID", "FECHA_REPORTE", "TIPO_INCIDENTE", "GRAVEDAD"],
                    "JuegosSucio.csv": ["ID","DESC_CLASIFICACION", "MODELO", "ESTADO", "ACCESIBLE", "FECHA_INSTALACION",
                                       "tipo_juego" ], # TODO agregar "FECHA_INTERVENCION"
                    "MantenimientoSucio.csv": ["ID", "FECHA_INTERVENCION", "TIPO_INTERVENCION", "ESTADO_PREVIO",
                                               "ESTADO_POSTERIOR"],
                    "meteo24.csv": ["PROVINCIA", "MUNICIPIO"],
                    "UsuariosSucio.csv": ["NIF", "NOMBRE", "EMAIL", "TELEFONO"]
                    }


class CSVProcessor:
    def __init__(self, directorio):
        self.directorio = directorio
        self.ruta_archivo = None


    def check_coordenates(self, latitud, longitud):
        """Comprobar el formato de """


    def delete_columns(self, df):
        """Funcion que elimina las columnas innecesarias de los archivos csv"""

        for nombre in NOMBRES_ARCHIVOS.keys():
            if nombre in self.ruta_archivo:

                # Considerar las columnas relevantes

                # En caso de ser el archivo de areas, crear el atributo coordenadas
                if nombre == "AreasSucio.csv":
                    if 'LATITUD' in df.columns and 'LONGITUD' in df.columns:
                        try:
                            df.loc[:, 'COORDENADAS'] = df['LATITUD'].astype(str) + ', ' + df['LONGITUD'].astype(str)
                            df = df.drop(['LATITUD', 'LONGITUD'], axis=1)
                        except Exception:
                            pass

                df = df[NOMBRES_ARCHIVOS[nombre]]

                # Devolver el datagrama
                return df

        return df


    def cargar_y_reemplazar_csv(self, ruta_archivo):
        self.ruta_archivo = ruta_archivo        # Actualizarlo segun el archivo
        df = pd.read_csv(ruta_archivo)

        # Borrar informacion innecesaria
        df = self.delete_columns(df)

        for columna in df.columns:
            df[columna] = df[columna].map(
                lambda x: 'None' if pd.isna(x) or str(x).strip() == '' else self.corregir_tipografia(x))

            if 'fecha' in columna.lower():
                try:
                    df[columna] = df[columna].map(lambda x: self.corregir_fecha(x))
                    print(f"Columna '{columna}' estandarizada al formato de fecha ISO.")
                except Exception as e:
                    print(f"Error al convertir la columna '{columna}': {e}")

            elif 'telefono' in columna.lower():
                df[columna] = df[columna].map(lambda x: self.formatear_telefono(x))


        # Quitar las instancias duplicadas
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
        except UserWarning:
            pass

        try:
            return pd.to_datetime(fecha, format='%d/%m/%Y', errors='raise').isoformat()
        except UserWarning:
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

    def procesar_archivos_csv(self):
        for archivo in os.listdir(self.directorio):
            if archivo.endswith('.csv'):
                ruta_archivo = os.path.join(self.directorio, archivo)
                self.cargar_y_reemplazar_csv(ruta_archivo)


# Directorio donde están los archivos .csv
directorio = './Datasets Práctica 1.2-20241015'

# Crear una instancia de la clase CSVProcessor
procesador_csv = CSVProcessor(directorio)

# Procesar todos los archivos .csv en el directorio
procesador_csv.procesar_archivos_csv()
