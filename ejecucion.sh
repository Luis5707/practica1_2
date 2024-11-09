#!/bin/bash

#1.Descomprime el zip con los ficheros sin procesar
unzip Datasets_Practica_1.2.zip -d Datasets_Practica_1.2

#2.Limpieza de ficheros
python3 cleanData.py

#3.Reinicia la base de datos por si no está vacia
mongosh <<EOF
use parque_recreativo;
db.dropDatabase()
EOF

#4.Importa los ficheros a MongoDB
mongoimport --db parque_recreativo --collection areaRecreativa --type csv --file Datasets_Practica_1.2/AreasSucio.csv --headerline
mongoimport --db parque_recreativo --collection encuestaSatisfaccion --type csv --headerline --file Datasets_Practica_1.2/EncuestasSatisfaccionSucio.csv
mongoimport --db parque_recreativo --collection incidencia --type csv --headerline --file Datasets_Practica_1.2/IncidenciasUsuariosSucio.csv
mongoimport --db parque_recreativo --collection incidenteSeguridad --type csv --headerline --file Datasets_Practica_1.2/IncidentesSeguridadSucio.csv
mongoimport --db parque_recreativo --collection usuario --type csv --headerline --file Datasets_Practica_1.2/UsuariosSucio.csv
mongoimport --db parque_recreativo --collection juego --type csv --headerline --file Datasets_Practica_1.2/JuegosSucio.csv
mongoimport --db parque_recreativo --collection mantenimiento --type csv --headerline --file Datasets_Practica_1.2/MantenimientoSucio.csv
mongoimport --db parque_recreativo --collection registroClima --type csv --headerline --file Datasets_Practica_1.2/MeteoModified.csv
mongoimport --db parque_recreativo --collection estaciones --type csv --headerline --file Datasets_Practica_1.2/estaciones_meteo_CodigoPostal.csv

#5.Ejecuta todo el script de MongoDB
mongosh --quiet < /practica1_2/migrations.txt

echo "Ejecucion finalizada"
