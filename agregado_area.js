// Agregado: AreaRecreativa_Clima
use parque_recreativo;

// ** Crea la colección areaRecreativa_Clima ** 
db.areaRecreativa.aggregate([
    {
        $lookup: { // Obtiene la estacion de clima asociada al área según el COD_POSTAL
            from: "registroClima",
            localField: "COD_POSTAL", 
            foreignField: "COD_POSTAL",
            as: "CLIMA"
        }
    },
    {
        $lookup: {  // Obtiene los juegos asociados al área según NDP
            from: "juego", 
            localField: "NDP", 
            foreignField: "NDP",
            as: "JUEGOS"
        } 
    },
    {
        $lookup: { // Obtiene los incidentes asociados al área según el ID
            from: "incidenteSeguridad",
            localField: "ID", 
            foreignField: "AREA_RECREATIVA_ID",
            as: "INCIDENTES"
        }
    },
    {
        $lookup: { // Obtiene las encuestas asociadas al área según el ID
            from: "encuestaSatisfaccion",
            localField: "ID", 
            foreignField: "AREA_RECREATIVA_ID",
            as: "ENCUESTAS"
        }
    },
    {
        $project: {
            ID: 1,
            JUEGOS: 1,
            INCIDENTES: 1,
            CLIMA: 1,
            "ENCUESTAS.ID": 1,
            "ENCUESTAS.COMENTARIOS": 1,
        }
    },
    {
        $out: { db: "parque_recreativo", coll: "areaRecreativa_Clima_AGREGADO" }
    }
]);

// ** Añade la columna de la puntuación **
db.areaRecreativa_Clima_AGREGADO.aggregate([
    {
        $addFields: { // Calcula la puntuación según el comentario
            PUNTUACION: {
                $map: {
                    input: "$ENCUESTAS.COMENTARIOS",
                    as: "comentario",
                    in: {
                        $switch: {
                            branches: [
                                { case: { $eq: ["$$comentario", "EXCELENTE"] }, then: 5 },
                                { case: { $eq: ["$$comentario", "MUY BUENO"] }, then: 4 },
                                { case: { $eq: ["$$comentario", "ACEPTABLE"] }, then: 3 },
                                { case: { $eq: ["$$comentario", "REGULAR"] }, then: 2 },
                                { case: { $eq: ["$$comentario", "DEFICIENTE"] }, then: 1 }
                            ],
                            default: 0
                        }
                    }
                }
            }
        }
    },
    {
        $addFields: {   
            PUNTUACION_COMENTARIOS: { $sum: "$PUNTUACION" }, // Suma la puntuación de cada comentario de cada encuesta para un juego
            JUEGOS_EN_REPARACION: {   // Calcula el número de juegos en reparación
                $size: {
                    $filter: {
                        input: "$JUEGOS",
                        as: "juego",
                        cond: { $eq: ["$$juego.ESTADO", "EN REPARACION"] }
                    }
                }
            },
            TOTAL_INCIDENTES_SEGURIDAD: { // Calcula el número de incidentes que hay por área
                $size: "$INCIDENTES" 
            }
        }
    },
    {
        $addFields: {   // Calcula la puntuación total
            ESTADO_GLOBAL_AREA: {
                $subtract: [
                    { $subtract: ["$PUNTUACION_COMENTARIOS", "$JUEGOS_EN_REPARACION"] },
                    "$TOTAL_INCIDENTES_SEGURIDAD"
                ]
            }
        }
    },
    {
        $project: {
            _id: 1,
            ID: 1,
            JUEGOS: 1,
            INCIDENTES: 1,
            CLIMA: 1,
            "ENCUESTAS.ID": 1,
            "ENCUESTAS.COMENTARIOS": 1,
            ESTADO_GLOBAL_AREA: 1
        }
    },
    {
        $out: { db: "parque_recreativo", coll: "areaRecreativa_Clima_AGREGADO" }
    }
]);

db.areaRecreativa_Clima_AGREGADO.aggregate([
    {
        $set: {
        CANTIDAD_JUEGOS_POR_TIPO: {
            $arrayToObject: {
            $map: {
                input: { $setUnion: "$JUEGOS.TIPO_JUEGO" },
                as: "tipo",
                in: {
                k: "$$tipo",
                v: {
                    $size: {
                    $filter: {
                        input: "$JUEGOS",
                        cond: { $eq: ["$$this.TIPO_JUEGO", "$$tipo"] }
                    }
                    }
                }
                }
            }
            }
        }
        }
    },
    {
        $project: {
            _id: 0,
            ID: 1,
            "JUEGOS.ID": 1,
            "INCIDENTES.ID": 1,
            "INCIDENTES.FECHA_REPORTE": 1,
            "INCIDENTES.TIPO_INCIDENTE": 1,
            "INCIDENTES.GRAVEDAD": 1,
            "CLIMA.ID": 1,
            "ENCUESTAS.ID": 1,
            ESTADO_GLOBAL_AREA: 1,
            CANTIDAD_JUEGOS_POR_TIPO: 1
        }
    },
    {
        $out: { db: "parque_recreativo", coll: "areaRecreativa_Clima_AGREGADO" }
    }
]);
