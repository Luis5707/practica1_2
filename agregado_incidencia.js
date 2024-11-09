// Agregado: Incidencia_AGREGADO
use parque_recreativo;
db.incidencia.aggregate([
    {
        $lookup: {
            from: "usuario",
            let: { usuarioIDs: "$USUARIO_ID" },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $in: ["$NIF", "$$usuarioIDs"] // Verifica si el NIF está en la lista de usuarioIDs
                        }
                    }
                }
            ],
            as: "USUARIOS"
        }
    },

    {
        $addFields: {
            NIVEL_ESCALAMIENTO: {
                        $switch: {
                            branches: [
                                { case: { $eq: ["$TIPO_INCIDENCIA", "EXCELENTE"] }, then: 5 },
                                { case: { $eq: ["$TIPO_INCIDENCIA", "DESGASTE"] }, then: 4 },
                                { case: { $eq: ["$TIPO_INCIDENCIA", "VANDALISMO"] }, then: 3 },
                                { case: { $eq: ["$TIPO_INCIDENCIA", "ROTURA"] }, then: 2 },
                                { case: { $eq: ["$TIPO_INCIDENCIA", "MAL FUNCIONAMIENTO"] }, then: 1 }
                            ],
                            default: 0
                        }
                    }
                }
    },

    {
        $project: {
            _id: 0,
            ID: 1,
            USUARIOS: 1,
            NIVEL_ESCALAMIENTO: 1
        }
    },

    {
        $out: {
            db: "parque_recreativo",
            coll: "incidencia_AGREGADO"
        }
    }
]);