from difflib import get_close_matches
import pandas as pd
import numpy as np
import os
from datetime import datetime
import unicodedata
import re
# TODO -- Cambiar formato de mantenimiento ID

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
                         "CONTRATO_COD","MODELO","TIPO_JUEGO","ACCESIBLE", "INDICADOR_EXPOSICION"], # TODO agregar "FECHA_INTERVENCION"

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
    

    def cargar_y_reemplazar_csv(self):
        """Metodo que carga y remplaza cada uno de los archivos.csv"""
        df = pd.read_csv(self.ruta_archivo)

        # Borrar informacion innecesaria
        if 'meteo24' not in self.ruta_archivo and 'estaciones' not in self.ruta_archivo:
            df = self.modify_columns(df)
        
        # Rellenar datos incompletos de Juegos y Areas
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
                        registro = self.direccion_auxiliar(registro, comparador)
                    # Rellena con el juego / area correspondiente
                    elif "AreasSucio" in self.ruta_archivo:
                        registro = self.cambiar_registro(registro, "/JuegosSucio.csv", comparador)
                    else:
                        registro = self.cambiar_registro(registro, "/AreasSucio.csv", comparador)

                    df.loc[df['ID'] == registro['ID'], ['TIPO_VIA']] = registro['TIPO_VIA']
                    df.loc[df['ID'] == registro['ID'], ['NOM_VIA']] = registro['NOM_VIA']
                    df.loc[df['ID'] == registro['ID'], ['NUM_VIA']] = registro['NUM_VIA']

        for columna in df.columns:
            if 'COD_DISTRITO' in columna or 'COD_POSTAL' in columna:
                try:
                    df[columna] = df[columna].map(lambda x: self.corregir_codigos(x))
                    print(f"Columna '{columna}' corregida.")
                except Exception as e:
                    print(f"Error al convertir la columna '{columna}': {e}")


            if 'fecha' in columna.lower():
                try:
                    df[columna] = df[columna].map(lambda x: self.corregir_fecha(x))
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
            if "MantenimientoSucio.csv" in self.ruta_archivo:
                # Cambia el orden del id de mantenimiento
                if 'id' in columna.lower():
                    try:
                        df[columna] = df[columna].map(lambda x: self.corregir_id(x))
                        print(f"Columna '{columna}' corregida.")
                    except Exception as e:
                        print(f"Error al convertir la columna '{columna}': {e}")
            if "IncidenciasUsuariosSucio.csv" in self.ruta_archivo:
                # Pasa el string a tipo lista
                if 'mantenimiento_id' in columna.lower():
                    try:
                        df[columna] = df[columna].map(lambda x: list(x))
                        print(f"Columna '{columna}' corregida.")
                    except Exception as e:
                        print(f"Error al convertir la columna '{columna}': {e}")



        # Quitar las instancias duplicadas
        if 'ID' in df.columns:
            df = df.drop_duplicates(subset='ID', keep='first')
            print("Duplicados en la columna 'ID' eliminados.")

        if 'meteo24'not in self.ruta_archivo:
            df.to_csv(self.ruta_archivo, index=False)
        elif 'meteo24' in self.ruta_archivo:
            self.modificar_meteo(df)
        
        #df.to_csv(self.ruta_archivo, index=False)

        print(
            f"Archivo '{self.ruta_archivo}' procesado y guardado con valores nulos, fechas estandarizadas y errores tipográficos corregidos.")


    def corregir_id(self, id):
        """Método que modifica el formato de id de MantenimientoSucio.csv"""
        aux = id.split()

        return aux[1] + aux[0][:-3]


    def corregir_fecha(self, fecha):
        """Funcion que corrige el formato de las fechas en el archivo.csv al estilo de MongoDB --> YYYY-MM-DD"""
        
        # Lista de formatos posibles
        formatos = [
            '%d/%m/%Y',             # Formato 20/10/2013
            '%Y-%m-%d %H:%M:%S',    # Formato 2018-06-01 00:00:00
            '%Y/%m/%d',             # Formato 08/02/16
            '%d-%m-%y',             # Formato 08-02-16
            '%d-%m-%Y',             # Formato 08-02-2016
            '%m-%d-%Y',             # Formato 02-08-2016
            '%Y-%m-%d',             # Formato 2016-16-02
            '%m/%d/%Y'              # Formato 02/08/2016    
        ]

        for formato in formatos:
            try:
                # Intenta convertir la fecha al formato ISO
                return pd.to_datetime(fecha, format=formato).isoformat()
            except (ValueError, TypeError):
                continue  # Continúa al siguiente formato si hay un error

        if not fecha or fecha == 'FECHA_INCORRECTA':
            return None  # Retorna directamente si la fecha es incorrecta

        return None  # Si ninguno de los formatos funciona


    def formatear_telefono(self, numero):
        # Eliminar cualquier carácter no numérico
        numero = re.sub(r'\D', '', str(numero))

        # Verificar que el número tiene al menos 9 o 10 dígitos
        if len(numero):
            return f'+{numero[:2]} {numero[2:5]} {numero[5:8]} {numero[8:]}'
        else:
            # Si el número no tiene el formato adecuado, retornarlo tal cual
            return numero


    def sustituir_puntos_y_comas(self ):
        # Leer el archivo CSV usando ';' como separador
        df = pd.read_csv(self.ruta_archivo, sep=';')
        
        # Guardar el archivo CSV usando ',' como separador, sobrescribiendo el original
        df.to_csv(self.ruta_archivo, sep=',', index=False)
        print(f"Archivo '{self.ruta_archivo}' modificado con éxito.")
    
    def corregir_codigos(self, codigo):
        """Funcion que corrige el formato de los codigos conviertiendolo a tipo entero."""
        try:
            # Intenta convertir la fecha al formato ISO
            if not pd.isna(codigo):
                return str(codigo) [:-2]
        except (ValueError, TypeError):
            return None  # Continúa al siguiente formato si hay un error

    def corregir_tipografia(self, texto):

        try:
            if isinstance(texto, str):
                texto = texto.upper()       # Convertir a mayusculas
                texto = unicodedata.normalize('NFKD', texto).encode('ascii', 'ignore').decode('utf-8')
                return texto
        except Exception as e:
            print(f"Error al corregir tipografía: {e}")
            return texto
        return texto

    def pasar_a_lista(self, valor):
        return list(valor)


    def procesar_archivos_csv(self):
        for archivo in os.listdir(self.directorio):
            if archivo.endswith('.csv'):
                self.ruta_archivo = os.path.join(self.directorio, archivo)

                if "meteo24" in self.ruta_archivo or 'estaciones' in self.ruta_archivo:
                    self.sustituir_puntos_y_comas() # Sustituir los ; por ,
                
                self.cargar_y_reemplazar_csv()


    def direccion_auxiliar(self, registro: dict, comparador: list):
        #Registro ausente: 4795141
        
        if 'TIPO_VIA' in comparador:
            registro['TIPO_VIA'] = self.tipo_via(registro)

        if 'NOM_VIA' in comparador:
            registro['NOM_VIA'] = self.nom_via(registro)

        if 'NUM_VIA' in comparador:
            registro['NUM_VIA'] = self.num_via(registro)

        return registro


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


    def cambiar_registro(self, registro: dict, archivo_destino: str, comparador: list):
        """Función que busca el registro coincidente en el archivo de destino"""
        df_destino = pd.read_csv(directorio + archivo_destino)

        for registro_aux in df_destino.to_dict(orient='records'):
            # Encuentra el juego correspondiente
            if registro['NDP'] == registro_aux['NDP']:
                for elemento in comparador:
                    aux = registro_aux[elemento]
                    #Guarda el registro si no es nulo
                    if aux:
                        registro[elemento] = aux

        return registro



    #def cambiar_registro(self, registro: dict, archivo_destino: str, comparador: list):
    #    """Función que busca el registro coincidente en el archivo de destino"""
#
    #    registro_aux = self.encontrar_registro(registro['LATITUD'], registro['LONGITUD'], archivo_destino, comparador)
#
    #    if registro_aux:
    #        for elemento in comparador:
    #            aux = registro_aux[elemento]
    #            #Guarda el registro si no es nulo
    #            if aux:
    #                registro[elemento] = aux
#
    #    return registro
#
    #def encontrar_registro(self, latitud, longitud, archivo_destino, comparador):
    #    """Función que encuentra las coordenadas"""
    #    df_destino = pd.read_csv(directorio + archivo_destino)
#
    #    # Redondea a 3 decimales para que pueda coincidir con algún valor
    #    latitud = "{:.3f}".format(latitud)
    #    longitud = "{:.3f}".format(longitud)
#
    #    for registro in df_destino.to_dict(orient='records'):
    #        latitud_aux = "{:.3f}".format(registro['LATITUD'])
    #        longitud_aux = "{:.3f}".format(registro['LONGITUD'])
#
    #        # Encuentra un registro con las mismas coordenadas
    #        if latitud_aux == latitud and longitud_aux == longitud:
    #            vacio = False
    #            for elemento in comparador:
    #                # Si contiene un elemento vacio, sale de la función
    #                if pd.isna(registro[elemento]):
    #                    vacio = True
    #                    break
#
    #            if not vacio:
    #                return registro
    #    
    #    return None
    

    def modificar_meteo(self, df):
        """Método que crea un nuevo csv con los datos necesarios en el archivo meteo24.csv"""
        csv = []

        # Recorre todos los registros de meteo
        for registro in df.to_dict(orient='records'):
            
            magnitud = registro['MAGNITUD']

            # Comprueba que es viento, precipitación o temperatura+
            if magnitud not in [81, 83, 89]:
                continue # Salta a la siguiente iteración

            for i in range(1,31):
                
                # Guarda la fecha
                if i < 10:
                    string_i = '0' + str(i)
                else:
                    string_i = str(i)

                dia = 'D'+ string_i

                fecha = str(i) + '-' + str(registro['MES']) + '-' + str(registro['ANO'])

                # Localiza la estación a la que pertenece
                estacion = registro['ESTACION']
              
                # Miramos si la fecha ya estaba en otro registro
                
                fechas = [d['FECHA'] for d in csv if d['DISTRITO'] == estacion]
                if not fechas:
                    fechas = []

                if fecha not in fechas:
                    registro_nuevo = True
                    # Crea un registro nuevo
                    registro_aux = {
                        'FECHA': fecha,
                        'DISTRITO': estacion
                    }
                else:
                    registro_nuevo = False
                    registro_aux = next((d for d in csv if d['FECHA'] == fecha and d['DISTRITO'] == estacion), None)

                  

                # Comprueba el tipo
                if magnitud == 81: # Viento
                    # Viento fuerte
                    if registro[dia] > 13.9:
                        registro_aux['VIENTO'] = True
                    else:   # Viento debil
                        registro_aux['VIENTO'] = False
                elif magnitud == 83:    # Temperatura
                    registro_aux['TEMPERATURA'] = registro[dia]

                else:   # Precipitacion
                    registro_aux['PRECIPITACION'] = registro[dia]
                

                if registro_nuevo:
                    # Añadir el registro porque es nuevo
                    csv.append(registro_aux)
                else:
                    # Buscar y modificar el registro guardado
                    for elem in csv:
                        if elem['FECHA'] == registro_aux['FECHA'] and elem['DISTRITO'] == registro_aux['DISTRITO']:
                            elem.update(registro_aux)

         # Convertir a datagrama
        df_estacion = pd.DataFrame(csv)

        # Guardar la informacion en un archivo.csv
        df_estacion.to_csv(self.directorio +'/MeteoModified.csv', index=False)

    #def modificar_meteo(self, df):
    #    """Esta funcion crea un nuevo csv con los datos necesarios en el archivo meteo24.csv"""
    #    
    #    df_estaciones = pd.read_csv(self.directorio+'/estaciones_meteo_CodigoPostal.csv')
    #    
    #    df_estaciones_dict = df_estaciones.to_dict(orient='records')
    #    
    #    lista_aux = []
    #    # Recorrer todas las filas
    #    for estacion in df_estaciones_dict:
    #        lista_fechas = []
    #        
    #        for registro in df.to_dict(orient = 'records'):
    #            codigo_registro = registro['PUNTO_MUESTREO'].split("_")[0]
    #            actualizado = False # Para comprobar que es el mismo registro    
    #            if estacion['CODIGO'] == codigo_registro and registro['MAGNITUD'] in ['81', '83', '89']:
    #                month = registro['MES']
    #                year = registro['ANO']
    #                fecha = month + '-' + year
    #                
    #                if fecha not in lista_fechas:
    #                    lista_fechas.append(fecha)
    #                    # Crear nuevo registro
#
    #                    registro_nuevo = {
    #                        'FECHA': month + '-' + year,
    #                        'AREA_ID': estacion['CODIGO_POSTAL'],   # TODO ---CORREGIR
    #                    }
#
    #                if registro['MAGNITUD'] == '81':      # VELOCIDAD_VIENTO
    #                    registro_aux = self.info_dias(registro, 'VV')
    #                    registro_nuevo.update(registro_aux)
    #                
    #                elif registro['MAGNITUD'] == '83':    # TEMPERATURA
    #                    registro_aux = self.info_dias(registro, 'T')
    #                    registro_nuevo.update(registro_aux)
    #                
    #                else:                                 # PRECIPITACION
    #                    registro_aux = self.info_dias(registro, 'P')
    #                    registro_nuevo.update(registro_aux)
#
    #                actualizado = True # Se ha modificado un registro 
    #              
    #            if actualizado:
    #                lista_aux.pop()
    #            
    #            # Guardar el registro en la lista
    #            lista_aux.append(registro_nuevo)
#
    #    # Convertir a datagrama
    #    df_estacion = pd.DataFrame(lista_aux)
#
    #    # Guardar la informacion en un archivo.csv
    #    df_estacion.to_csv(directorio +'/MeteoModified.csv', index=False)

                

    #def info_dias(self, registro, tipo):
    #    """Funcion que devuelve en forma de lista toda la informacion sobre un registro de cada dia"""
    #    registro_aux = {}
    #    # Tomamos los encabezados de los dias
    #    for elem in registro.keys()[7:]:
    #        if 'D' in elem:
    #            registro_aux[tipo + elem[1:]] = registro[elem]
    #    return registro_aux
        

# Directorio donde están los archivos .csv
directorio = 'Datasets_Practica_1.2'

# Crear una instancia de la clase CSVProcessor
procesador_csv = CSVProcessor(directorio)

# Procesar todos los archivos .csv en el directorio
procesador_csv.procesar_archivos_csv()
