db.createCollection("areaRecreativa", { // Validación areaRecreativa
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las áreas',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                DESC_CLASIFICACION: {
                    bsonType: 'string',
                    description: "desc_clasificacion debe ser un string"
                },
                COD_BARRIO: {
                    bsonType: 'int',
                    description: "cod_barrio debe ser un int"
                },
                BARRIO: {
                    bsonType: 'string',
                    description: "barrio debe ser un string"
                },
                COD_DISTRITO: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        {bsonType: "int"},
                        {pattern: "^[0-9]+-COD_DISTRITO-ausente$", bsonType: "string"}
                    ],
                    description: "cod_distrito debe ser un int"
                },
                DISTRITO: {
                    bsonType: 'string'
                 },
                COORD_GIS_X: {
                    bsonType: 'double',
                    description: "Las coordenadas x deben de ser double"
                },
                COORD_GIS_Y: {
                    bsonType: 'double',
                    description: "Las coordenadas y deben de ser double"
                },
                SISTEMA_COORD: {
                    bsonType: 'string',
                    enum: ['ETRS89'],
                    description: "sistema_coord debe ser un string ETRS89"
                },
                LATITUD: {
                    bsonType: 'double',
                    minimum: -90,
                    maximum: 90,
                    description: "Latitud debe ser un valor double entre -90 y 90"
                },
                LONGITUD: {
                    bsonType: 'double',
                    minimum: -180,
                    maximum: 180,
                    description: "longitud debe ser un valor double entre -180 y 180"
                },
                TIPO_VIA: {     // Hay que revisar lo de ausente
                    bsonType: 'string',
                    oneOf: [
                        { enum: ["CALLE", "AUTOVIA", "AVENIDA", "CARRETERA", "PARQUE", "PASEO", "PLAZA", "GLORIETA", "BULEVAR", "TRAVESIA", "CAMINO", "CUESTA", "PASAJE", "RONDA"] },
                        { pattern: "^[0-9]+-TIPO_VIA-ausente$", bsonType: "string" }
                    ],
                    description: "Tipo_via debe ser un string"
                },
                NOM_VIA: {
                    bsonType: 'string',
                    description: "nom_via debe ser un string"
                },
                NUM_VIA: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        { bsonType: "int" },
                        { bsonType: "string" }
                    ],
                    description: "num_via debe ser un int"
                },
                COD_POSTAL: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        { bsonType: "int" },
                        { pattern: "^[0-9]+-COD_POSTAL-ausente$", bsonType: "string" }
                    ],
                    description: "cod_postal debe ser un valor int entre 10000 y 99999"
                },
                DIRECCION_AUX: {
                    bsonType: 'string',
                    description: "direccion_aux debe ser un string"
                },
                NDP: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        { bsonType: "int" },
                        { pattern: "^[0-9]+-NDP-ausente$", bsonType: "string" }
                    ],
                    description: "NDP debe ser un int"
                },
                FECHA_INSTALACION: {
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date"
                },
                CODIGO_INTERNO: {
                    bsonType: 'string',
                    oneOf: [
                        { bsonType: "string", pattern: "^D[0-9]+$" },
                        { pattern: "^[0-9]+-CODIGO_INTERNO-ausente$", bsonType: "string" }
                    ],
                    description: "codigo_interno debe ser un string de la forma Dxxxx"
                },
                CONTRATO_COD: {
                    bsonType: 'string',
                    enum: ['AE21', '6'],
                    description: "contrato_cod debe ser un string AE21"
                },
                TOTAL_ELEMENTO: {
                    bsonType: 'int',
                    description: "total_elemento debe ser un int"
                },
                TIPO: {
                    bsonType: 'string',
                    description: "tipo debe ser un string"
                }
            }
        }
    }
});

db.createCollection("encuestaSatisfaccion", { // Validación de encuestasSatisfaccion
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las encuestas realizadas por los usuarios a cada área',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                PUNTUACION_ACCESIBILIDAD: {
                    bsonType: 'int',
                    minimum: 0,
                    maximum: 5,
                    description: "puntuacion_accesibilidad debe ser un int entre 0 y 5"
                },
                PUNTUACION_CALIDAD: {
                    bsonType: 'int',
                    minimum: 0,
                    maximum: 5,
                    description: "puntuacion_calidad debe ser un int entre 0 y 5"

                },
                COMENTARIOS: {
                    bsonType: 'string',
                    enum: ['DEFICIENTE', 'REGULAR', 'ACEPTABLE', 'MUY BUENO', 'EXCELENTE'],
                    description: "comentarios debe ser un string"
                },
                AREA_RECREATIVA_ID: {
                    bsonType: 'int',
                    description: "area_recreativa_id debe ser un int"
                },
                FECHA: {
                    bsonType: ['date', 'string'],
                    oneOf:[
                        {bsonType: 'date'},
                        {bsonType: 'string'},
                    ],
                    description: "fecha debe ser un date"
                }
            }
        }
    }
});


db.createCollection('juego', {
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene los juegos de cada área',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                DESC_CLASIFICACION: {
                    bsonType: "string",
                    description: "desc_clasificacion debe ser un string"
                },
                COD_BARRIO: {
                    bsonType: 'int',
                    description: "cod_barrio debe ser un int"
                },
                BARRIO: {
                    bsonType: 'string'
                },
                COD_DISTRITO: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        {bsonType: "int"},
                        {pattern: "^[0-9]+-COD_DISTRITO-ausente$", bsonType: "string"}
                    ],
                },
                DISTRITO: {
                    bsonType: 'string'
                },
                ESTADO: {
                    bsonType: 'string',
                    enum: ["OPERATIVO", "EN REPARACION"]
                },
                COORD_GIS_X: {
                    bsonType: 'double',
                    description: "Las coordenadas y deben de ser double"
                },
                COORD_GIS_Y: {
                   bsonType: 'double',
                    description: "Las coordenadas y deben de ser double"
                },
                SISTEMA_COORD: {
                    bsonType: 'string',
                    enum: ['ETRS89'],
                    description: "sistema_coord debe ser un string ETRS89"
                },
                LATITUD: {
                    bsonType: 'double',
                    minimum: -90,
                    maximum: 90
                },
                LONGITUD: {
                    bsonType: 'double',
                    minimum: -180,
                    maximum: 180
                },
                TIPO_VIA: {     // Hay que revisar lo de ausente-----------------TODO añadir
                    bsonType: 'string',
                    enum: ["CALLE", "AUTOVIA", "AVENIDA", "CARRETERA", "PARQUE", "PASEO", "PLAZA"]
                },
                NOM_VIA: {          // TODO --- añadir
                    bsonType: 'string'
                },
                NUM_VIA: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        { bsonType: "int" },
                        { bsonType: "string" }
                    ],
                    description: "num_via debe ser un int o string"
                },
                COD_POSTAL: {
                    bsonType: ['int', 'string'],
                    oneOf: [
                        { bsonType: "int" },
                        { pattern: "^[0-9]+-COD_POSTAL-ausente$", bsonType: "string" }
                    ],
                    description: "cod_postal debe ser un valor int o string"
                },
                DIRECCION_AUX: {                //TODO ---- añadir
                    bsonType: 'string',
                    description: "direccion_aux debe ser un string"
                },
                NDP: {
                    bsonType: ['double', 'string'],
                    oneOf: [
                        { bsonType: "double" },
                        { pattern: "^[0-9]+-NDP-ausente$", bsonType: "string" }
                    ],
                    description: "NDP debe ser un int"
                },
                FECHA_INSTALACION: {
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        { pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date" 
                },
                CODIGO_INTERNO: {
                    bsonType: ['string', 'int'],
                    description: "codigo_interno debe ser un string o int"
                },
                CONTRATO_COD: {
                    bsonType: ['string', 'int'],
                    
                    oneOf: [
                        { enum: ['AE21'] },
                        { bsonType: 'int', enum: [6] },
                        { pattern: "^[0-9]+-COD_POSTAL-ausente$", bsonType: "string" }
                    ],
                },
                MODELO: {               
                    bsonType: "string",
                    description: "Modelo debe ser un string"
                },
                ACCESIBLE: {            
                    bsonType: "bool"
                },
                INDICADOR_EXPOSICION: {
                    bsonType: "string",
                    enum: ["BAJO", "ALTO", "MEDIO"]
                }
            }
        }
    }
});


db.nombreColeccion.aggregate([
    { $project: {
        ACCESIBLE: { $type: "$campo1" },
        campo2: { $type: "$campo2" },
        campo3: { $type: "$campo3" }
    }},
    { $limit: 10 } // Muestra los tipos de los primeros 10 documentos
]);
