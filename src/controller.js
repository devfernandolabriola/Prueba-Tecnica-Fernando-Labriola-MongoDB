const Records = require('./records.model');
const fs = require('fs');
const csv = require('fast-csv');

const upload = async (req, res) => {
    const {file} = req;

    /* Tamaño del batch para insertar en MongoDB en bloques de 1000*/
    const BATCH_SIZE = 1000;
    let batch = []

    /* Crea un stream para leer el archivo CSV y parsearlo con encabezados */
    const stream = fs.createReadStream(file.path).pipe(csv.parse({ headers: true }))
        /* Evento que se dispara por cada fila parseada del CSV */
        stream.on('data',async(data) => {
            batch.push(data)
            /* Cuando el batch alcanza el tamaño definido, pausamos el stream y hacemos la inserción */
            if (batch.length >= BATCH_SIZE) {
                stream.pause();
                try {
                    await Records.insertMany(batch);
                    batch = [];
                } catch (error) {
                    console.error('Error insertando batch:', error);
                }
                stream.resume();
            }
        })
        
        /* Evento que se dispara al finalizar la lectura del archivo */
        stream.on('end',async() => {
            try {
                /* Insertar cualquier dato restante que no completó un batch */
                if (batch.length > 0) {
                    await Records.insertMany(batch);
                }

                /* Borrar el archivo temporal para liberar espacio en disco */
                fs.unlink(file.path, (e) => {
                    if (e) console.error('No se pudo borrar el archivo:', e);
                });

                return res.status(200).json({ message: 'Los datos se agregaron correctamente' });
            } catch (error) {
                console.error('Error final al insertar o limpiar:', e);
                return res.status(500).json({ error: 'Error al finalizar el procesamiento' });
            }
            
        })
        
         /* Evento que se dispara si hay un error en la lectura o parseo del archivo */
        stream.on('error', (e) => {
            console.error('Error leyendo el archivo CSV:', e);
            return res.status(500).json({ error: 'Error leyendo el archivo' });
        });

};

const list = async (_, res) => {
    try {
        const data = await Records
            .find({})
            .limit(10)
            .lean();
        
        return res.status(200).json(data);
    } catch (err) {
        return res.status(500).json(err);
    }
};

module.exports = {
    upload,
    list,
};
