// Agregado: Juego_AGREGADO
use parque_recreativo;

            
// ** Crea el agregado juegoMantenimiento **
db.juego.aggregate([
    {
        $lookup: {
            from: "mantenimiento",
            localField: "ID",
            foreignField: "JUEGO_ID",
            as: "MANTENIMIENTOS"
        }
    },
    {
        $addFields: {
            mantenimientoIDs: {
                $map: {
                    input: "$MANTENIMIENTOS",
                    as: "mantenimiento",
                    in: "$$mantenimiento.ID"
                }
            }
        }
    },
    {
        $lookup: {
            from: "incidencia",
            let: { mantenimientoIDs: "$mantenimientoIDs" },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $gt: [
                                { $size: { $setIntersection: ["$MANTENIMIENTO_ID", "$$mantenimientoIDs"] } },
                                0
                            ] // Verifica si hay intersección entre MANTENIMIENTO_ID y mantenimientoIDs
                        }
                    }
                },
                {
                    $sort: { FECHA_REPORTE: 1 } // Ordena las incidencias por FECHA_REPORTE en orden ascendente
                }
            ],
            as: "INCIDENCIAS"
        }
    },
    {
      $addFields: {
        TIEMPO_DE_USO: { $add: [{ $floor: { $multiply: [ { $rand: {} }, 15 ] } }, 1] }
      }
    },
    {
      $addFields: {
        DESGASTE_ACUMULADO: {
          $subtract: [
            {
              $multiply: [ "$TIEMPO_DE_USO",
                {
                  $switch: {
                    branches: [
                      { case: { $eq: ["$INDICADOR_EXPOSICION", "BAJO"] }, then: 1 },
                      { case: { $eq: ["$INDICADOR_EXPOSICION", "MEDIO"] }, then: 2 },
                      { case: { $eq: ["$INDICADOR_EXPOSICION", "ALTO"] }, then: 3 }
                    ],
                    default: 1 // Valor predeterminado en caso de un valor no reconocido
                  }
                }
              ]
            },
            {
              $size: "$MANTENIMIENTOS" 
            }
          ]
        }
      }
    },
    {
        $project: {
            _id: 0,
            ID: 1,
            MANTENIMIENTOS: 1,
            INCIDENCIAS: 1,
            DESGASTE_ACUMULADO: 1
        }
    },
    {
        $out: {
            db: "parque_recreativo",
            coll: "juego_AGREGADO"
        }
    }
]);

// ** Añade el campo ultimoMantenimiento

db.juego_AGREGADO.aggregate([
    {
        $addFields: {
            ULTIMO_MANTENIMIENTO: {
                $reduce: {
                    input: "$MANTENIMIENTOS",
                    initialValue: null,
                    in: {
                        $cond: {
                            if: {
                                $or: [
                                    { $eq: ["$$value", null] },
                                    { $gt: [{ $toDate: "$$this.FECHA_INTERVENCION" }, { $toDate: "$$value.FECHA_INTERVENCION" }] }
                                ]
                            },
                            then: "$$this",
                            else: "$$value"
                        }
                    }
                }
            }
        }
    },
    {
        $addFields: {
            ULTIMA_FECHA_MANTENIMIENTO: {
                $cond: {
                    if: { $ne: ["$ULTIMO_MANTENIMIENTO", null] },
                    then: { $toDate: "$ULTIMO_MANTENIMIENTO.FECHA_INTERVENCION" },
                    else: null
                }
            }
        }
    },
    {
        $addFields: {
            PRIMERA_INCIDENCIA: {
                $arrayElemAt: [
                    {
                        $filter: {
                            input: "$INCIDENCIAS",
                            as: "incidencia",
                            cond: {
                                $in: ["$ULTIMO_MANTENIMIENTO.ID", "$$incidencia.MANTENIMIENTO_ID"]
                            }
                        }
                    },
                    0
                ]
            }
        }
    },
    {
        $addFields: {
            PRIMERA_FECHA_REPORTE: {
                $cond: {
                    if: { $ne: ["$PRIMERA_INCIDENCIA", null] },
                    then: { $toDate: "$PRIMERA_INCIDENCIA.FECHA_REPORTE" },
                    else: null
                }
            }
        }
    },
    {
        $addFields: {
            TIEMPO_RESOLUCION: {
                $cond: {
                    if: {
                        $and: [
                            { $ne: ["$ULTIMA_FECHA_MANTENIMIENTO", null] },
                            { $ne: ["$PRIMERA_FECHA_REPORTE", null] }
                        ]
                    },
                    then: {
                        $dateDiff: {
                            startDate: "$PRIMERA_FECHA_REPORTE",
                            endDate: "$ULTIMA_FECHA_MANTENIMIENTO",
                            unit: "day"
                        }
                    },
                    else: null // Si no hay fechas válidas, el resultado es null
                }
            }
        }
    },
    {
        $project: {
            _id: 0,
            ID: 1,
            "MANTENIMIENTOS.ID": 1,
            "INCIDENCIAS.ID": 1,
            "INCIDENCIAS.TIPO_INCIDENCIA": 1,
            "INCIDENCIAS.FECHA_REPORTE": 1,
            "INCIDENCIAS.ESTADO": 1,
            DESGASTE_ACUMULADO: 1,
            ULTIMA_FECHA_MANTENIMIENTO: 1,
            TIEMPO_RESOLUCION: 1
        }
    },
    {
        $out: {
            db: "parque_recreativo",
            coll: "juego_AGREGADO"
        }
    }
]);