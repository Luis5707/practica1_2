use parque_recreativo;
// ** Convierte a lista MANTENIMIENTO_ID **
db.incidencia.updateMany(
   {},
   [
      {
         $set: {
            MANTENIMIENTO_ID: {
               $function: {
                  body: function(str) {
                     // Remover corchetes y comillas simples
                     const cleanStr = str.replace(/[\[\]']/g, "");
                     // Dividir en elementos individuales usando la coma y espacio
                     return cleanStr.split(", ").map(item => item.trim());
                  },
                  args: ["$MANTENIMIENTO_ID"],
                  lang: "js"
               }
            }
         }
      }
   ]
);

// ** Convierte a lista USUARIO_ID **
db.incidencia.updateMany(
   {},
   [
      {
         $set: {
            USUARIO_ID: {
               $function: {
                  body: function(str) {
                     // Remover corchetes y comillas simples
                     const cleanStr = str.replace(/[\[\]']/g, "");
                     // Dividir en elementos individuales usando la coma y espacio
                     return cleanStr.split(", ").map(item => item.trim());
                  },
                  args: ["$USUARIO_ID"],  // Corregido aquí
                  lang: "js"
               }
            }
         }
      }
   ]
);

// Convierte a formato 'date' FECHA_INTERVENCION en mantenimiento
db.mantenimiento.aggregate([
    {
        $addFields: {
            FECHA_INTERVENCION: {
                $cond: {
                    if: { $ne: ["$FECHA_INTERVENCION", null] },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA_INTERVENCION",
                            format: "%Y-%m-%dT%H:%M:%S" // Formato para tu fecha en el CSV
                        }
                    },
                    else: "$FECHA_INTERVENCION"
                }
            }
        }
    },
    {
        $out: "mantenimiento" // Guarda los documentos modificados en la misma colección
    }
]);


// Convierte a formato 'date' FECHA_REPORTE en incidencia
db.incidencia.aggregate([
    {
        $addFields: {
            FECHA_REPORTE: {
                $cond: {
                    if: { $ne: ["$FECHA_REPORTE", null] },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA_REPORTE",
                            format: "%Y-%m-%dT%H:%M:%S" // Formato para tu fecha en el CSV
                        }
                    },
                    else: "$FECHA_REPORTE"
                }
            }
        }
    },
    {
        $out: "incidencia" // Guarda los documentos modificados en la misma colección
    }
]);

// Convierte a formato 'date' FECHA en registroClima
db.registroClima.aggregate([
    {
        $addFields: {
            FECHA: {
                $cond: {
                    if: { $ne: ["$FECHA", null] },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA"
                        }
                    },
                    else: "$FECHA"
                }
            }
        }
    },
    {
        $out: "registroClima" // Guarda los documentos modificados en la misma colección
    }
]);

// Convierte a formato 'date' FECHA en encuestaSatisfaccion
db.encuestaSatisfaccion.aggregate([
    {
        $addFields: {
            FECHA: {
                $cond: {
                    if: { $ne: ["$FECHA", null] },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA"
                        }
                    },
                    else: "$FECHA"
                }
            }
        }
    },
    {
        $out: "encuestaSatisfaccion" // Guarda los documentos modificados en la misma colección
    }
]);

// Convierte a formato 'date' FECHA_REPORTE en IncidenteSeguridad
db.incidenteSeguridad.aggregate([
    {
        $addFields: {
            FECHA_REPORTE: {
                $cond: {
                    if: { $ne: ["$FECHA_REPORTE", null] },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA_REPORTE",
                            format: "%Y-%m-%dT%H:%M:%S" // Formato para tu fecha en el CSV
                        }
                    },
                    else: "$FECHA_REPORTE"
                }
            }
        }
    },
    {
        $out: "incidenteSeguridad" // Guarda los documentos modificados en la misma colección
    }
]);

// Conveierte a formato 'date' FECHA_INSTALACION en AreaRecreativa
db.areaRecreativa.aggregate([
    {
        $addFields: {
            FECHA_INSTALACION: {
                $cond: {
                    if: {
                        $and: [
                            { $ne: ["$FECHA_INSTALACION", null] },
                            { $not: { $regexMatch: { input: "$FECHA_INSTALACION", regex: /ausente/ } } }
                        ]
                    },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA_INSTALACION",
                            format: "%Y-%m-%dT%H:%M:%S" // Formato para tu fecha en el CSV
                        }
                    },
                    else: "$FECHA_INSTALACION"
                }
            }
        }
    },
    {
        $out: "areaRecreativa" // Guarda los documentos modificados en la misma colección
    }
]);

// Convierte a formato 'date' FECHA_INSTALACION en Juego
db.juego.aggregate([
    {
        $addFields: {
            FECHA_INSTALACION: {
                $cond: {
                    if: {
                        $and: [
                            { $ne: ["$FECHA_INSTALACION", null] },
                            { $not: { $regexMatch: { input: "$FECHA_INSTALACION", regex: /ausente/ } } }
                        ]
                    },
                    then: {
                        $dateFromString: {
                            dateString: "$FECHA_INSTALACION",
                            format: "%Y-%m-%dT%H:%M:%S" // Formato para tu fecha en el CSV
                        }
                    },
                    else: "$FECHA_INSTALACION"
                }
            }
        }
    },
    {
        $out: "juego" // Guarda los documentos modificados en la misma colección
    }
]);

db.registroClima.aggregate([
    {
        $lookup: {
            from: "estaciones",   
            localField: "CODIGO", 
            foreignField: "CODIGO",
            as: "estacion"   
        }
    },
    {
        $unwind: {
            path: "$estacion",
            preserveNullAndEmptyArrays: true 
        }
    },
    {
        $addFields: {
            COD_POSTAL: "$estacion.CODIGO_POSTAL" 
        }
    },
    {
        $project: {
            estacion: 0
        }
    },
    {
        $out: "registroClima"
    }

]);
