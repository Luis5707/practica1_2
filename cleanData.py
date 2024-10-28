from difflib import get_close_matches
import pandas as pd
import numpy as np
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
#CLASE_JUEGO = ["ID","DESC_CLASIFICACION", "MODELO", "ESTADO", "ACCESIBLE", "FECHA_INSTALACION", "TIPO_JUEGO", "FECHA_INTERVENCION", "LATITUD", "LONGITUD"] # TODO desgasteAcumulado, indicadorExposicion no sabemos-- suponemos ultimaFechaMantenimiento de Mantenimiento
#CLASE_MANTENIMIENTO = ["ID", "FECHA_INTERVENCION", "TIPO_INTERVENCION", "ESTADO_PREVIO", "ESTADO_POSTERIOR"]
##CLASE_REGISTRO_CLIMA = [""] TODO -- No sabemos que poner aqui

NOMBRES_ARCHIVOS = {"AreasSucio.csv":
                        ["ID","DESC_CLASIFICACION","COD_BARRIO","BARRIO","COD_DISTRITO","DISTRITO","ESTADO","COORD_GIS_X","COORD_GIS_Y","SISTEMA_COORD",
                        "COORDENADAS","TIPO_VIA","NOM_VIA","NUM_VIA","COD_POSTAL","DIRECCION_AUX","NDP","FECHA_INSTALACION","CODIGO_INTERNO","CONTRATO_COD",
                        "TOTAL_ELEM","TIPO"], # tipo

                    "EncuestasSatisfaccionSucio.csv":
                        ["ID","PUNTUACION_ACCESIBILIDAD","PUNTUACION_CALIDAD","COMENTARIOS","AREA_RECREATIVA_ID","FECHA"],

                    "IncidenciasUsuariosSucio.csv": 
                        ["ID", "TIPO_INCIDENCIA", "FECHA_REPORTE", "ESTADO", "USUARIO_ID", "MANTENIMIENTO_ID"], #UsuarioID, MantenimientoID
                    
                    "IncidentesSeguridadSucio.csv": 
                        ["ID", "FECHA_REPORTE", "TIPO_INCIDENTE", "GRAVEDAD", "AREA_RECREATIVA_ID"], #AreaRecreativaID

                    "JuegosSucio.csv": 
                        ["ID","DESC_CLASIFICACION","COD_BARRIO","BARRIO","COD_DISTRITO","DISTRITO","ESTADO","COORD_GIS_X","COORD_GIS_Y","SISTEMA_COORD",
                         "COORDENADAS","TIPO_VIA","NOM_VIA","NUM_VIA","COD_POSTAL","DIRECCION_AUX","NDP","FECHA_INSTALACION","CODIGO_INTERNO",
                         "CONTRATO_COD","MODELO","TIPO_JUEGO","ACCESIBLE", "INDICADOR_EXPOSICION"], # tipo_juego, INDICADOR_EXPOSICION TODO agregar "FECHA_INTERVENCION"

                    "MantenimientoSucio.csv": 
                        ["ID","FECHA_INTERVENCION","TIPO_INTERVENCION","ESTADO_PREVIO","ESTADO_POSTERIOR","JUEGO_ID","TIPO","COMENTARIOS"], #JuegoID, Tipo, Comentarios
                    
                    "meteo24.csv": 
                        ["PROVINCIA", "MUNICIPIO", "ESTACION", "MAGNITUD", "PUNTO_MUESTREO", "ANO" ,"MES", "V01", "V02", "V03", "V04", "V05","V06", "V07", "V08", 
                            "V09", "V10", "V11", "V12", "V13", "V14", "V15",  "V16", "V17",  "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", 
                            "V26", "V27", "V28", "V29", "V30", "V31"],
                    
                    "UsuariosSucio.csv": ["NIF", "NOMBRE", "EMAIL", "TELEFONO"] # descartamos email falso
                    }


class CSVProcessor:
    def __init__(self, directorio):
        self.directorio = directorio
        self.ruta_archivo = None
        

    def value_indicador_juego(self, df):
        """Valores aleatorios a la columna INDICADOR_EXPOSICION en el archivo JuegosSucio.csv (BAJO, MEDIO, ALTO)"""
        df['INDICADOR_EXPOSICION'] = np.random.choice(['BAJO', 'MEDIO', 'ALTO'], size=len(df))
        return df

    
    def modify_columns(self, df):
        """Funcion que elimina las columnas innecesarias de los archivos csv"""

        for nombre in NOMBRES_ARCHIVOS.keys():
            if nombre in self.ruta_archivo:

                # En caso de ser el archivo de areas, crear el atributo coordenadas
                if nombre == "AreasSucio.csv":
                    df = self.cambio_nombre('tipo', 'TIPO', df)
                
                if nombre == "EncuestasSatisfaccionSucio.csv" or nombre == "IncidentesSeguridadSucio.csv":
                    df = self.cambio_nombre("AreaRecreativaID", "AREA_RECREATIVA_ID", df)

                if nombre == "JuegosSucio.csv":
                    df = self.cambio_nombre('tipo_juego', 'TIPO_JUEGO', df)
                    # Crear valores aleatorios en JuegosSucio.csv
                    df = self.value_indicador_juego(df)

                if nombre == "IncidenciasUsuariosSucio.csv":
                    df = self.cambio_nombre('MantenimeintoID', 'MANTENIMIENTO_ID', df)
                    df = self.cambio_nombre('UsuarioID', 'USUARIO_ID', df)
                
                if nombre == "MantenimientoSucio.csv":
                    df = self.cambio_nombre('Tipo', 'TIPO', df)
                    df = self.cambio_nombre('JuegoID', 'JUEGO_ID', df)
                    df = self.cambio_nombre('Comentarios', 'COMENTARIOS', df)

                if nombre == "AreasSucio.csv" or nombre == "JuegosSucio.csv":
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

    def cambio_nombre(self, nombre_antiguo, nombre_nuevo, df):
        """Función que cambia los nombres de los atributos"""
        if nombre_antiguo in df.columns:
            try:
                df.loc[:, nombre_nuevo] = df[nombre_antiguo].astype(str)
                return df.drop([nombre_antiguo], axis=1)
            except Exception:
                pass
        return df
    

    def cargar_y_reemplazar_csv(self, ruta_archivo):
        """Metodo que carga y remplaza cada uno de los archivos.csv"""
        self.ruta_archivo = ruta_archivo        # Actualizarlo segun el archivo
        df = pd.read_csv(ruta_archivo)

        # Borrar informacion innecesaria
        df = self.modify_columns(df)

        #TODO -- Antes de este bucle, se debe asignar las direcciones en juegos y areas (tipo_via, direccion_aux, etc).

        for columna in df.columns:
            """ TODO -- Corregir
            if "UsuariosSucio.csv" not in self.ruta_archivo and "meteo24.csv" not in self.ruta_archivo:
            # Si no es "UsuariosSucio.csv" ni "meteo24.csv", usar la columna 'ID'
                df[columna] = df[columna].map(
                    lambda x: f"{df['ID']}-{columna}-ausente" if pd.isna(x) or str(x).strip() == '' else self.corregir_tipografia(x)
                )
            elif "UsuariosSucio.csv" in self.ruta_archivo:
                # Si es "UsuariosSucio.csv", usar la columna 'NIF'
                df[columna] = df[columna].map(
                    lambda x: f"{df['NIF']}-{columna}-ausente" if pd.isna(x) or str(x).strip() == '' else self.corregir_tipografia(x)
                )
            """


            if 'fecha' in columna.lower():
                try:
                    df[columna] = df[columna].map(lambda x: self.corregir_fecha(x, df, columna))
                    print(f"Columna '{columna}' estandarizada al formato de fecha ISO.")
                except Exception as e:
                    print(f"Error al convertir la columna '{columna}': {e}")

            elif 'telefono' in columna.lower():
                df[columna] = df[columna].map(lambda x: self.formatear_telefono(x))

            elif 'cod_distrito' in columna.lower() or 'cod_postal' in columna.lower():
                try:
                    df[columna] = df[columna].astype(int)# TODO tenemos que hacer que coincida con lo de area si hay valor nulo
                except:
                    pass

        # Quitar las instancias duplicadas
        if 'ID' in df.columns:
            df = df.drop_duplicates(subset='ID', keep='first')
            print("Duplicados en la columna 'ID' eliminados.")

        df.to_csv(ruta_archivo, index=False)
        print(
            f"Archivo '{ruta_archivo}' procesado y guardado con valores nulos, fechas estandarizadas y errores tipográficos corregidos.")



    def corregir_fecha(self, fecha, df, columna):
        if fecha == 'None':
            return df["ID"]+ "-" + columna.upper() + "-ausente" 

        try:
            return pd.to_datetime(fecha, errors='raise').isoformat()
        except UserWarning:
            pass

        try:
            return pd.to_datetime(fecha, format='%d/%m/%Y', errors='raise').isoformat()
        except UserWarning:
            pass

        return df["ID"]+ "-" + columna.upper() + "-ausente" 

    def formatear_telefono(self, numero):
        # Eliminar cualquier carácter no numérico
        numero = re.sub(r'\D', '', str(numero))

        # Verificar que el número tiene al menos 9 o 10 dígitos
        if len(numero):
            return f'+{numero[:2]} {numero[2:5]} {numero[5:8]} {numero[8:]}'
        else:
            # Si el número no tiene el formato adecuado, retornarlo tal cual
            return numero

    def sustituir_puntos_y_comas(ruta_archivo):
        # Leer el archivo CSV usando ';' como separador
        df = pd.read_csv(ruta_archivo, sep=';')
        
        # Guardar el archivo CSV usando ',' como separador, sobrescribiendo el original
        df.to_csv(ruta_archivo, sep=',', index=False)
        print(f"Archivo '{ruta_archivo}' modificado con éxito.")
    
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
                

                #TODO --- Corregir
                """
                if "meteo24" in archivo:
                    self.sustituir_puntos_y_comas(ruta_archivo) # Sustituir los ; por ,
                """
                self.cargar_y_reemplazar_csv(ruta_archivo)


# Directorio donde están los archivos .csv
directorio = 'Datasets_Practica_1_2'

# Crear una instancia de la clase CSVProcessor
procesador_csv = CSVProcessor(directorio)

# Procesar todos los archivos .csv en el directorio
procesador_csv.procesar_archivos_csv()
