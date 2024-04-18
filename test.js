"use strict";

const cola_redis = require('.');
const redis = require('redis');
const db_redis = redis.createClient('redis://172.31.19.108:6379?db=13');

var envios = new cola_redis('pruebas_cola',db_redis,async function(data) {

        let aleatorio = Math.random();             
        // Generar Errores de Forma Aleatoria
        if (aleatorio<0.2) {            
            throw 'Error Aleatorio. Reintentar Orden: '+data.orden+' '+data.nombre;
        }
        return 'Procesado Correctamente Orden: '+data.orden+' '+data.nombre;                        
});

envios.envios_max_segundo=0;
envios.procesos_maximos=7;
envios.reintento_numero=5;
envios.reintento_segundos=10;

envios.on('error',function(proceso) {
    console.error('*** Evento de ERROR',proceso);
})

envios.on('reintento',function(proceso) {
    console.error('R** Evento de REINTENTO Orden:'+proceso.data.orden+' '+proceso.data.nombre+' en '+proceso.intentos+' intentos. '+new Date());
})

envios.on('procesado',function(proceso) {    
   console.log('P** Evento de PROCESADO Orden:'+proceso.data.orden+' '+proceso.data.nombre+' en '+proceso.intentos+' intentos. '+new Date());
})

envios.on('start',function() {    
    console.log('Evento Cola Iniciada: '+new Date());
})

envios.start();

// Añadir Elementos a la cola
for (let i = 0; i < 15; i++) {
    envios.add({'nombre':'Pedro','apellidos':'Casas','orden':i});
}
for (let i = 0; i < 10; i++) {
    envios.add_lenta({'nombre':'Carolina','apellidos':'Casas','orden':i});
}
for (let i = 16; i < 30; i++) {
    envios.add({'nombre':'Jose','apellidos':'Casas','orden':i});
}
for (let i = 0; i < 5; i++) {
    envios.add_rapida({'nombre':'Miguel Angel','apellidos':'Casas','orden':i});
}

//envios.start();

envios.en_cola().then(function(result) {
    console.log('En Cola Promesa: '+result);
});

(async () => {
    console.log('En Espera: '+await envios.en_espera());
    console.log('En Reintento: '+await  envios.en_reintento());
    console.log('En Cola Async: '+await  envios.en_cola());
    console.log('Lanzados: '+envios.lanzados());
    console.log('Ultimo: '+envios.ultimo());
  })();
