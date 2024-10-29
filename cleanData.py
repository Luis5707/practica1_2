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


lista_autovia = ['AUTOVIA', 'AUTOV']
lista_calle = ['CALLE', 'C/', 'C.', 'CL.', 'C']
lista_avenida = ['AVENIDA', 'AVDA', 'AV']
lista_carretera = ['CARRETERA', 'CTRA']
lista_parque = ['PARQUE', 'PQ']
lista_paseo = ['PASEO', 'PO']
lista_plaza = ['PLAZA', 'PZA']
tipos_via = [lista_autovia, lista_avenida, lista_calle, lista_carretera, lista_parque, lista_paseo, lista_plaza]


NOMBRES_ARCHIVOS = {"AreasSucio.csv":
                        ["ID","DESC_CLASIFICACION","COD_BARRIO","BARRIO","COD_DISTRITO","DISTRITO","ESTADO","COORD_GIS_X","COORD_GIS_Y","SISTEMA_COORD",
                        "LATITUD", "LONGITUD", "TIPO_VIA","NOM_VIA","NUM_VIA","COD_POSTAL","DIRECCION_AUX","NDP","FECHA_INSTALACION","CODIGO_INTERNO","CONTRATO_COD",
                        "TOTAL_ELEM","TIPO"], # tipo

                    "EncuestasSatisfaccionSucio.csv":
                        ["ID","PUNTUACION_ACCESIBILIDAD","PUNTUACION_CALIDAD","COMENTARIOS","AREA_RECREATIVA_ID","FECHA"],

                    "IncidenciasUsuariosSucio.csv": 
                        ["ID", "TIPO_INCIDENCIA", "FECHA_REPORTE", "ESTADO", "USUARIO_ID", "MANTENIMIENTO_ID"], #UsuarioID, MantenimientoID
                    
                    "IncidentesSeguridadSucio.csv": 
                        ["ID", "FECHA_REPORTE", "TIPO_INCIDENTE", "GRAVEDAD", "AREA_RECREATIVA_ID"], #AreaRecreativaID

                    "JuegosSucio.csv": 
                        ["ID","DESC_CLASIFICACION","COD_BARRIO","BARRIO","COD_DISTRITO","DISTRITO","ESTADO","COORD_GIS_X","COORD_GIS_Y","SISTEMA_COORD",
                         "LATITUD", "LONGITUD", "TIPO_VIA","NOM_VIA","NUM_VIA","COD_POSTAL","DIRECCION_AUX","NDP","FECHA_INSTALACION","CODIGO_INTERNO",
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
        # Comprobar que son los archivos Juegos y Areas
        if 'JuegosSucio' in self.ruta_archivo or 'AreasSucio' in self.ruta_archivo:
            #Buscar considerando un diccionario los datos
            for registro in df.to_dict(orient = 'records'):
                comparador = []
                rellenar = False
                if pd.isna(registro['TIPO_VIA']):
                    comparador.append('TIPO_VIA')
                    rellenar = True
                if pd.isna(registro['NOM_VIA']):
                    comparador.append('NOM_VIA')
                    rellenar = True
                if pd.isna(registro['NUM_VIA']):
                    comparador.append('NUM_VIA')
                    rellenar = True
                
                # Rellena los campos correspondientes
                if rellenar:
                    # Rellena con la dirección auxiliar si no está vacía
                    if not pd.isna(registro['DIRECCION_AUX']):
                        df = self.direccion_auxiliar(registro, comparador, df)
                    # Rellena con el juego / area correspondiente
                    if "AreasSucio" in self.ruta_archivo:
                        df = self.encontrar_registro(registro, "/JuegosSucio.csv", comparador, df)
                    else:
                        df = self.encontrar_registro(registro, "/AreasSucio.csv", comparador, df)

        for columna in df.columns:

            if "UsuariosSucio.csv" not in self.ruta_archivo and "meteo24.csv" not in self.ruta_archivo:
                # Si no es "UsuariosSucio.csv" ni "meteo24.csv", usar el valor en la columna 'ID'
                df[columna] = df.apply(
                    lambda row: f"{row['ID']}-{columna}-ausente" if pd.isna(row[columna]) or str(row[columna]).strip() == '' else self.corregir_tipografia(row[columna]),
                    axis=1
                )
            elif "UsuariosSucio.csv" in self.ruta_archivo:
                # Si es "UsuariosSucio.csv", usar el valor en la columna 'NIF'
                df[columna] = df.apply(
                    lambda row: f"{row['NIF']}-{columna}-ausente" if pd.isna(row[columna]) or str(row[columna]).strip() == '' else self.corregir_tipografia(row[columna]),
                    axis=1
                )


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


    def sustituir_puntos_y_comas(self, ruta_archivo):
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
                
                if "meteo24" in ruta_archivo:
                    self.sustituir_puntos_y_comas(ruta_archivo) # Sustituir los ; por ,
                
                self.cargar_y_reemplazar_csv(ruta_archivo)


    def direccion_auxiliar(self, registro: dict, comparador: list, df):
        #Registro ausente: 4795141
        
        if 'TIPO_VIA' in comparador:
            result = self.tipo_via(registro)
            df.loc[df['ID'] == registro['ID'], ['TIPO_VIA']] = result
        if 'NOM_VIA' in comparador:
            result = self.nom_via(registro)
            df.loc[df['ID'] == registro['ID'], ['NOM_VIA']] = result
        if 'NUM_VIA' in comparador:
            result = self.num_via(registro)
            df.loc[df['ID'] == registro['ID'], ['NUM_VIA']] = result

        return df


    def tipo_via(self, registro:dict):
        """Función que rellena el campo TIPO_VIA del registro a partir de la dirección auxiliar"""
        dir_aux = registro['DIRECCION_AUX']
        palabras = dir_aux.split()

        for lista in tipos_via:
            if palabras[0] in lista:
                
                return lista[0]
            
        return None


    def num_via(self, registro):
        """Función que rellena el campo NUM_VIA del registro a partir de la dirección auxiliar"""
        dir_aux = registro['DIRECCION_AUX']
    
        # Separar por palabras
        palabras = dir_aux.split()
        
        lista_aux = []
        coma_encontrada = False

        for palabra in palabras:
            if coma_encontrada:
                lista_aux.append(palabra)
            if ',' in palabra:
                coma_encontrada = True

        #Si había coma
        if coma_encontrada:
            try:
                entero_aux = int(lista_aux[0])
                lista_aux[0] = str(entero_aux)
                return ' '.join(lista_aux)
            except:
                pass
        
        return None


    def nom_via(self, registro):
        """Función que rellena el campo NOM_VIA del registro a partir de la dirección auxiliar"""
        dir_aux = registro['DIRECCION_AUX']

        # Separar por palabras
        palabras = dir_aux.split()
        primera_palabra = palabras[0]

        #Comprobar si la primera palabra pertenece a una lista de palabras

        for lista in tipos_via:
            if primera_palabra in lista:
                palabras = palabras[1:]
        
        lista_aux = []

        for palabra in palabras:
            if ',' in palabra:
                palabra = palabra[:-1]
                lista_aux.append(palabra)
                break
            elif palabra == 'CON':
                break
            lista_aux.append(palabra)

        return ' '.join(lista_aux)


    def encontrar_registro(self, registro: dict, archivo_destino: str, comparador: list, df):
        """Función que busca el registro coincidente en el archivo de destino"""
        df = pd.read_csv(directorio + archivo_destino)
        ndp_origen = registro['NDP']

        for fila in df.to_dict(orient='records'):
            ndp_destino = fila['NDP']

            # Encuentra el registro correspondiente
            if ndp_origen == ndp_destino:

                for elemento in comparador:
                    aux = fila[elemento]
                    #Guarda el registro si no es nulo
                    if aux:
                        df.loc[df['ID'] == registro['ID'], [elemento]] = aux
                        #registro[elemento] = aux

                return df
            
        return df



# Directorio donde están los archivos .csv
directorio = 'Datasets_Practica_1_2'

# Crear una instancia de la clase CSVProcessor
procesador_csv = CSVProcessor(directorio)

# Procesar todos los archivos .csv en el directorio
procesador_csv.procesar_archivos_csv()
