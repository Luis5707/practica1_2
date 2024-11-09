use parque_recreativo;

db.runCommand({ // Validación AreaRecreativa
    collMod: "areaRecreativa",
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
                TIPO_VIA: {
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

db.runCommand({ // Validación de EncuestaSatisfaccion
    collMod: "encuestaSatisfaccion", 
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
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        { pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date"
                }
            }
        }
    }
});

db.runCommand({ // Validación de Incidencia
    collMod: "incidencia",
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las incidencias reportadas por los usuarios de cada juego',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                TIPO_INCIDENCIA: {
                    bsonType: 'string',
                    enum: ["MAL FUNCIONAMIENTO", "DESGASTE", "ROTURA", "VANDALISMO"],
                    description: "tipo_incidencia debe ser un string"
                },
                FECHA_REPORTE: {
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        { pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date"
                },
                ESTADO: {
                    bsonType: 'string',
                    enum: ['ABIERTA', 'CERRADA'],
                    description: "estado debe ser un string con valor ABIERTA o CERRADA"
                },
                USUARIO_ID: {
                    bsonType: ['string', 'array'],
                    description: "usuario_id debe ser un string con formato de array"
                },
                MANTENIMIENTO_ID: {
                    bsonType: ['string', 'array'],
                    description: "mantenimiento_id debe ser un string con formato de array"
                }
            }
        }
    }
});

db.runCommand( { // Validacion de incidenteSeguridad
    collMod: "incidenteSeguridad",
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las incidencias de seguridad de cada área',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                FECHA_REPORTE: {
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        { pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date"
                },
                TIPO_INCIDENTE: {
                    bsonType: 'string',
                    enum: ["ROBO", "CAIDA", "ACCIDENTE", "VANDALISMO", "DANO ESTRUCTURAL"]
                },
                GRAVEDAD: {
                    bsonType: 'string',
                    enum: ["CRITICA", "BAJA", "MEDIA", "ALTA"]
                },
                AREA_RECREATIVA_ID: {
                    bsonType: 'int'
                }
            }
        }
    }
});

db.runCommand({    // Validacion de Usuario
    collMod: "usuario", 
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las áreas',
            required: ['NIF'],
            properties: {
                NIF: {
                    bsonType: 'string',
                    pattern: "^[0-9]{3}-[0-9]{2}-[0-9]{4}$",
                    description: "El id debe ser un int y es PK"
                },
                NOMBRE: {
                    bsonType: "string"
                },
                EMAIL: {
                    bsonType: "string"
                },
                TELEFONO: {
                    bsonType: "string",
                    pattern: "^\\+[0-9]{2} [0-9]{3} [0-9]{3} [0-9]{3}$"
                }
            }
        }
    }
});

db.runCommand({  // Validación de Juego
    collMod: "juego" ,
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
                    bsonType: ['double','string'],
                    oneOf: [
                        {bsonType: "double"},
                        {pattern: "^[0-9]+-COORD_GIS_X-ausente$", bsonType: "string"}
                    ],
                    description: "Las coordenadas y deben de ser double"
                },
                COORD_GIS_Y: {
                    bsonType: ['double','string'],
                    oneOf: [
                        {bsonType: "double"},
                        {pattern: "^[0-9]+-COORD_GIS_Y-ausente$", bsonType: "string"}
                    ],
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
                        { pattern: "^[0-9]+-CONTRATO_COD-ausente$", bsonType: "string" }
                    ],
                },
                MODELO: {             
                    bsonType: ["string", "double", "int"],
                    oneOf: [
                        { bsonType: 'int'},
                        { bsonType: 'double'},
                        { bsonType: "string" }
                    ],
                    description: "Modelo debe ser un string o entero"
                },
                ACCESIBLE: {           
                    bsonType: "string",
                    oneOf: [  
                        { enum: ['NO', 'SI'], bsonType: "string"},
                        { pattern: "^[0-9]+-ACCESIBLE-ausente$", bsonType: "string" }
                    ],
                },
                INDICADOR_EXPOSICION: {
                    bsonType: "string",
                    enum: ["BAJO", "ALTO", "MEDIO"]
                }
            }
        }
    }
});

db.runCommand({  // Validacion de Mantenimiento
    collMod: "mantenimiento",
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene las áreas',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'string',
                    pattern: "^MNT-[0-9]{5}$",
                    description: "El id debe ser un string y es PK"
                },
                FECHA_INTERVENCION: {
                    bsonType: ["date", "string"],
                    oneOf: [
                        { bsonType: "date" },
                        { pattern: "^[0-9]+-FECHA_INSTALACION-ausente$", bsonType: "string"},
                        { pattern: "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$", bsonType: "string"},
                        
                    ],
                    description: "Fecha_instalacion debe ser tipo date"
                },
                TIPO_INTERVENCION: {
                    bsonType: "string",
                    enum: ["CORRECTIVO", "PREVENTIVO", "EMERGENCIA"]
                },
                ESTADO_PREVIO: {
                    bsonType: "string",
                    enum: ["MALO", "REGULAR", "BUENO"]
                },
                ESTADO_POSTERIOR: {
                    bsonType: "string",
                    enum: ["MALO", "REGULAR", "BUENO"]
                },
                JUEGO_ID: {
                    bsonType: "int"
                },
                TIPO: {
                    bsonType: "string",
                    oneOf: [
                        {bsonType: "string", enum: ["PREVENTIVO", "QUEJA_USUARIO"]},
                        {bsonType: "string", pattern: "^MNT-[0-9]{5}+-TIPO-ausente$"}
                    ]
                },
                COMENTARIOS: {
                    bsonType: "string"
                }
            }
        }
    }
});


db.runCommand({  // Validación de RegistroClima
    collMod: "registroClima",
    validator: {
        $jsonSchema: {
            bsonType: 'object',
            description: 'Documento que contiene el registro del clima',
            required: ['ID'],
            properties: {
                ID: {
                    bsonType: 'int',
                    description: "El id debe ser un int y es PK"
                },
                FECHA: {
                    bsonType: ["string", "date"]
                },
                CODIGO: {
                    bsonType: "int"
                },
                TEMPERATURA: {
                    bsonType: "double"
                },
                PRECIPITACION: {
                    bsonType: "double"
                }
            }
        }
    }
});
