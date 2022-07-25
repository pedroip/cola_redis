"use strict";

const cola_redis = require('cola_redis');
const redis = require('redis');
const db_redis = redis.createClient('redis://xxx.xxx.xxx.xxx:6379?db=n');

var envios = new cola_redis('pruebas_cola',db_redis,async function(data) {

        let aleatorio = Math.random();             
        if (aleatorio<0.2) {            
            throw 'Error Aleatorio. Reintentar Orden: '+data.orden;
        }
        else {            
            return 'Procesado Correctamente Orden: '+data.orden;        
        }     
        
});

envios.envios_max_segundo=2;
envios.procesos_maximos=7;
envios.reintento_numero=5;
envios.reintento_segundos=60;

envios.on('error',function(proceso) {
    console.error('Evento de ERROR',proceso);
})

envios.on('reintento',function(proceso) {
    console.error('Evento de REINTENTO Orden:'+proceso.data.orden+' en '+proceso.intentos+' intentos. '+new Date());
})

envios.on('procesado',function(proceso) {    
   console.log('Evento de PROCESADO Orden:'+proceso.data.orden+' en '+proceso.intentos+' intentos. '+new Date());
})

envios.on('start',function() {    
    console.log('Evento Cola Iniciada: '+new Date());
})

// Añadir Elementos a la cola
for (let i = 0; i < 30; i++) {
    envios.add({'nombre':'Pedro','apellidos':'Casas','orden':i});
}

envios.start();

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
