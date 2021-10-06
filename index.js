"use strict";

const events = require('events');
const util = require('util');


function cola_redis(nombre_cola,conexion_redis,proceso_data) {

    var self = this; 

    this.activo = false; 

    this.proceso_data = proceso_data;

    this.db_redis = conexion_redis;

    this.nombre_cola = nombre_cola+':cola';
    this.procesos_maximos = 5;  
    this.procesos_actuales = 0;

    this.reintento_nombre_cola = nombre_cola+':reintentar';
    this.reintento_segundos = 120;
    this.reintento_numero = 6;

    this.envios_max_segundo = 0;
    this.envios_ultimo = null; // Valor en MS
    this.semaforo_espera_activa = false;

    this.ProcesoTimeout = null; 

    this.push_cola = function (paquete,callback) {

        self.db_redis.LPUSH(
            self.nombre_cola,
            JSON.stringify(paquete),
            function(err, reply) {			
                if (err) {
                    self.emit('error',{
                        'codigo':1,
                        'mensaje':'Error Push Cola',
                        'origen':err,
                        'intentos':paquete.intentos,
                        'data':paquete.data                                        
                    });                                				
                }			
                else if (callback && self.activo) {	                    							
                    callback(reply)
                }	
            }
        );     

    }

    this.push_cola_reintento = function (paquete) {
        paquete.intento_time = new Date().getTime();       
        self.db_redis.LPUSH(
            self.reintento_nombre_cola,
            JSON.stringify(paquete),
            function(err, reply) {			
                if (err) {
                    self.emit('error',{
                        'codigo':2,
                        'mensaje':'Error Push Reintento',   
                        'origen':err,                     
                        'intentos':paquete.intentos,
                        'data':paquete.data                        
                    });                                				
                }			                
            }
        );     
    } 

    this.check_cola = async function () {
        
        if (self.activo && self.procesos_actuales<self.procesos_maximos) {
            let paquete = null;
            try {              
                self.procesos_actuales++;
                let ahora = new Date();  

                // Control del ritmo Antes de leer de la cola
                if (self.envios_max_segundo>0) {
                    let pausa =  (1/self.envios_max_segundo)*1000;                                  
                    while (self.semaforo_espera_activa || (self.envios_ultimo && self.envios_ultimo+pausa>ahora.getTime()) ) {
                        await self.sleep(self.envios_ultimo+pausa-ahora.getTime()); 
                        ahora = new Date();
                    }
                }

                self.semaforo_espera_activa=true;
                paquete = JSON.parse(await util.promisify(self.db_redis.RPOP).bind(self.db_redis)(self.nombre_cola));                
                self.semaforo_espera_activa=false;

                if (paquete) {                    
                    try {                        
                        paquete.intentos++;                        
                        self.envios_ultimo = ahora.getTime();                                         
                        try {                                                        
                            let response_data = await self.proceso_data(paquete.data);		                                                        
                            self.emit('procesado',{
                                'codigo':0,
                                'mensaje':'Data Procesado Correctamente',
                                'respuesta':response_data,
                                'intentos':paquete.intentos,
                                'espera_ms':ahora.getTime()-paquete.created,
                                'data':paquete.data                                                                                                        
                            });
                        } catch (err) {                                                              
                            if (paquete.intentos>paquete.intentos_max) {                                
                                self.emit('error',{
                                    'codigo':6,
                                    'mensaje':'Error Maximos Reintentos Alcanzados',
                                    'origen':err,
                                    'intentos':paquete.intentos,
                                    'data':paquete.data                                                                        
                                });
                            }
                            else {   
                                self.push_cola_reintento(paquete); 
                                self.emit('reintento',{
                                    'codigo':7,
                                    'mensaje':'Añadido el Reintento del Procesado',
                                    'origen':err,
                                    'intentos':paquete.intentos,
                                    'data':paquete.data                                                                        
                                });                                                                                        
                            }
                        }     
                        
                    } catch (error) {                        
                        self.emit('error',{
                            'codigo':4,
                            'mensaje':'Error Call Procesado de Data',
                            'origen':error,
                            'intentos':paquete.intentos,
                            'data':paquete.data                                        
                        });                                                      
                    } 

                }                 
            } catch (error) {  
                self.semaforo_espera_activa=false;              
                self.emit('error',{
                    'codigo':5,
                    'mensaje':'Error Chequeo de Cola',
                    'origen':error                                                   
                });                  
            }    
            self.procesos_actuales--;
            if (paquete && self.activo) {
                self.check_cola();
            }
        }                
    }

    this.control_time = async function () {
        
        let proximo_control_time = self.reintento_segundos*1000;
        let reintento_fin=new Date().getTime()-proximo_control_time; 
        let paquete = null;
        do {
            paquete = JSON.parse(await util.promisify(self.db_redis.RPOP).bind(self.db_redis)(self.reintento_nombre_cola));              
            if (paquete) {
                if (paquete.intento_time<reintento_fin) {
                    self.push_cola(paquete);
                } else {
                    proximo_control_time = paquete.intento_time-reintento_fin;
                    self.db_redis.RPUSH(
                        self.reintento_nombre_cola,
                        JSON.stringify(paquete),
                        function(err, reply) {			
                            if (err) {
                                self.emit('error',{
                                    'codigo':3,
                                    'mensaje':'Error Push Retorno de Reintento',                                    
                                    'origen':err,                                    
                                    'intentos':paquete.intentos,
                                    'data':paquete.data                                    
                                });                                				
                            }			                
                        }
                    );    
                    paquete=null;
                }
            }    
        } while (paquete); 

        if (self.activo) {
            self.check_cola();   
        }  
         
        self.ProcesoTimeout = setTimeout(self.control_time,proximo_control_time);            
    }    

    this.sleep = function (ms) {        
        return new Promise((resolve) => {
           setTimeout(resolve, ms);
        });
    } 

    // Funciones Publica 
    // ----------------------------------------------------------------------------------------------
    
    // Añade un conjunto de datos a la cola para su procesado ordenado
    this.add = function (data,intentos_max=5) {

        let paquete = {
            "created":new Date().getTime(),
            "intentos":0,
            "intento_time":null,
            "intentos_max":intentos_max,            
            "data":data
        }
        
        self.push_cola(paquete,function() {   
            if (self.activo) {        
                self.check_cola();
            }    
        });
       
    }
    
    // Inicia o Reanida el procesado de la cola
    this.start = function () {
        self.activo = true;
        self.semaforo_espera_activa=false; 
        self.emit('start');
        self.control_time();
    }    

    // Detiene el procesamiento de la cola
    this.stop = function () {
        self.activo = false;
        if (self.ProcesoTimeout) {
            clearTimeout(self.ProcesoTimeout);
            self.ProcesoTimeout=null;
        }    
        self.emit('stop');
    }    

    // Para la cola y espera a que no queden procesos lanzados
    this.end = function () {
        self.stop();
        while (self.procesos_actuales>0) {
            self.sleep(1000);
        }
        self.emit('end');
    }

    // Numero de Procesos en Ejecuciuón
    this.lanzados =  function  () {
        return self.procesos_actuales;
    }

    // Numero de elementos en Cola (Total)
    this.en_cola =  async function  () {
        let total = 0;
        total = total + await self.en_espera();
        total = total + await self.en_reintento();
        return total;
    }

    // Numero de elementos en Espera
    this.en_espera =  async function  () {
        let paquetes = 0
        try {
            paquetes = await util.promisify(self.db_redis.LLEN).bind(self.db_redis)(self.nombre_cola); 
            if (isNaN(paquetes)) {
                paquetes = 0;
            }
        } catch (err) {
            paquetes = 0;   
        }    
        return paquetes;    
    }

    // Numero de elementos en Reintento
    this.en_reintento =  async function  () {
        let paquetes = 0
        try {
            paquetes = await util.promisify(self.db_redis.LLEN).bind(self.db_redis)(self.reintento_nombre_cola); 
            if (isNaN(paquetes)) {
                paquetes = 0;
            }
        } catch (err) {
            paquetes = 0;   
        }    
        return paquetes;
    }

    // Ultimo Envio
    this.ultimo =  function  () {
        return self.envios_ultimo;
    }

}

util.inherits(cola_redis,events);

module.exports = cola_redis;