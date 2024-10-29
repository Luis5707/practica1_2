#!/bin/bash

# Definir el nombre del archivo ZIP y el directorio de destino
ZIP_FILE="Datasets Práctica 1.2-20241015.zip"
DEST_DIR="Datasets_Practica_1_2"

# Ejecutar el comando unzip
unzip "$ZIP_FILE" -d "$DEST_DIR"

# Comprobar si el comando fue exitoso
if [ $? -eq 0 ]; then
    echo "Archivo descomprimido correctamente en $DEST_DIR"
else
    echo "Error al descomprimir el archivo"
fi