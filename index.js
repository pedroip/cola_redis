"use strict";

const events = require('events');
const util = require('util');


function cola_redis(nombre_cola,conexion_redis,proceso_data) {

    var self = this; 

    this.activo = false; 

    this.proceso_data = proceso_data;

    this.db_redis = conexion_redis;

    this.nombre_cola_rapida = nombre_cola+':rapida';
    this.nombre_cola = nombre_cola+':cola';
    this.nombre_cola_lenta = nombre_cola+':lenta';

    this.procesos_maximos = 5;  
    this.procesos_actuales = 0;

    this.reintento_nombre_cola = nombre_cola+':reintentar';
    this.reintento_segundos = 120;
    this.reintento_numero = 6;

    this.envios_max_segundo = 0;
    this.envios_ultimo = null; // Valor en MS
    this.semaforo_espera_activa = false;

    // Intervalo máximo entre chequeos de la cola, para detectar elementos añadidos por otras instancias
    this.chequeo_segundos = 1;

    this.ProcesoTimeout = null;
    // Se incrementa en cada start/stop para invalidar bucles de control anteriores
    this.ciclo_control = 0;

    // Normal. Rapida y Lenta N.R.L
    this.push_cola = function (paquete,tipo='N',callback) {

        var nombre_cola = self.nombre_cola;        
        if (tipo=='R') {
            nombre_cola = self.nombre_cola_rapida;              
        }
        else if (tipo=='L') {
            nombre_cola = self.nombre_cola_lenta;    
        }

        self.db_redis.LPUSH(
            nombre_cola,
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
                else {
                    if (callback && self.activo) {
                        callback(reply);
                    }
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
                        let espera = self.envios_ultimo ? self.envios_ultimo+pausa-ahora.getTime() : 0;
                        await self.sleep(Math.max(espera,5));
                        ahora = new Date();
                    }
                }


                //Gestion de Colas Preferentes
                //----------------------------
                // Se consultan siempre las tres listas, pueden tener datos de otras instancias o de antes del start
                if (self.activo) {
                    try {
                        self.semaforo_espera_activa=true;
                        paquete = await self.extraer(self.nombre_cola_rapida,'R');
                        if (paquete==null) {
                            paquete = await self.extraer(self.nombre_cola,'N');
                        }
                        if (paquete==null) {
                            paquete = await self.extraer(self.nombre_cola_lenta,'L');
                        }
                    } finally {
                        self.semaforo_espera_activa=false;
                    }
                }
                //-----------------------------


                if (paquete) {
                    try {
                        paquete.intentos++;
                        self.envios_ultimo = ahora.getTime();
                        // Hay datos: si queda capacidad se lanza otro proceso en paralelo
                        // (después de registrar envios_ultimo para respetar la cadencia)
                        if (self.activo && self.procesos_actuales<self.procesos_maximos) {
                            self.check_cola();
                        }
                        let response_data = null;
                        let error_proceso = null;
                        let fallo = false;
                        try {
                            response_data = await self.proceso_data(paquete.data);
                        } catch (err) {
                            fallo = true;
                            error_proceso = err;
                        }
                        // Los eventos se emiten fuera del try del proceso: un error en un oyente no debe provocar reintentos
                        if (!fallo) {
                            self.emit('procesado',{
                                'codigo':0,
                                'mensaje':'Data Procesado Correctamente',
                                'respuesta':response_data,
                                'intentos':paquete.intentos,
                                'espera_ms':ahora.getTime()-paquete.created,
                                'data':paquete.data
                            });
                        } else {
                            let err = error_proceso;
                            // Analizar Si queremo mas reintentos
                            if (err && typeof err === 'object' && err.no_reintentar && err.error ) {
                                self.emit('rechazado',{
                                    'codigo':9,
                                    'mensaje':'Error Sin Reintento',
                                    'origen':err.error,
                                    'intentos':paquete.intentos,
                                    'data':paquete.data                                                                        
                                });
                            }
                            else if (paquete.intentos>paquete.intentos_max) {                                
                                self.emit('rechazado',{
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

    // Extrae el paquete más antiguo de una lista anotando su tipo de origen (para conservarlo en los reintentos)
    this.extraer = async function (nombre_lista,tipo) {
        let paquete = JSON.parse(await util.promisify(self.db_redis.RPOP).bind(self.db_redis)(nombre_lista));
        if (paquete && typeof paquete === 'object') {
            paquete.tipo = tipo;
        }
        return paquete;
    }

    // Bucle de control: devuelve a su cola los reintentos vencidos y chequea la cola.
    // Solo sigue vivo mientras la cola esté activa y no haya habido otro start/stop (ciclo).
    this.control_time = async function (ciclo = self.ciclo_control) {

        self.ProcesoTimeout = null;
        let proximo_control_time = self.reintento_segundos*1000;
        try {
            let reintento_fin=new Date().getTime()-proximo_control_time;
            let paquete = null;
            do {
                paquete = null;
                // Se mira el más antiguo sin sacarlo; solo se extrae si ya ha vencido
                let siguiente = JSON.parse(await util.promisify(self.db_redis.LINDEX).bind(self.db_redis)(self.reintento_nombre_cola,-1));
                if (!siguiente) {
                    break;
                }
                if (siguiente.intento_time>=reintento_fin) {
                    proximo_control_time = siguiente.intento_time-reintento_fin;
                    break;
                }
                paquete = JSON.parse(await util.promisify(self.db_redis.RPOP).bind(self.db_redis)(self.reintento_nombre_cola));
                if (paquete) {
                    if (paquete.intento_time<reintento_fin) {
                        self.push_cola(paquete,paquete.tipo||'N');
                    } else {
                        // Otra instancia se adelantó y el extraído aún no ha vencido: se devuelve a su sitio
                        const devuelto = paquete;
                        proximo_control_time = devuelto.intento_time-reintento_fin;
                        self.db_redis.RPUSH(
                            self.reintento_nombre_cola,
                            JSON.stringify(devuelto),
                            function(err, reply) {
                                if (err) {
                                    self.emit('error',{
                                        'codigo':3,
                                        'mensaje':'Error Push Retorno de Reintento',
                                        'origen':err,
                                        'intentos':devuelto.intentos,
                                        'data':devuelto.data
                                    });
                                }
                            }
                        );
                        paquete=null;
                    }
                }
            } while (paquete && ciclo===self.ciclo_control);

            if (self.activo && ciclo===self.ciclo_control) {
                self.check_cola();
            }
        } catch (error) {
            self.emit('error',{
                'codigo':10,
                'mensaje':'Error Control de Reintentos',
                'origen':error
            });
        } finally {
            if (self.activo && ciclo===self.ciclo_control) {
                proximo_control_time = Math.min(proximo_control_time,self.chequeo_segundos*1000);
                if (proximo_control_time<100) {
                    proximo_control_time=100;
                }
                self.ProcesoTimeout = setTimeout(() => self.control_time(ciclo),proximo_control_time);
            }
        }
    }

    this.sleep = function (ms) {        
        return new Promise((resolve) => {
           setTimeout(resolve, ms);
        });
    } 

    // Funciones Publica 
    // ----------------------------------------------------------------------------------------------
    
    // Añade un conjunto de datos a la cola para su procesado ordenado
    this.add = function (data,intentos_max=-1,tipo='N') {

        if (intentos_max<0) {
            intentos_max=self.reintento_numero;
        }

        let paquete = {
            "created":new Date().getTime(),
            "intentos":0,
            "intento_time":null,
            "intentos_max":intentos_max,            
            "data":data
        }
             
        self.push_cola(paquete,tipo,function() {   
            if (self.activo) {        
                self.check_cola();
            }    
        });           
    }

    this.add_lenta = function (data,intentos_max=-1) {
        self.add(data,intentos_max,'L');     
    }

    this.add_rapida = function (data,intentos_max=-1) {
        self.add(data,intentos_max,'R');     
    }
               
    // Inicia o Reanida el procesado de la cola
    this.start = function () {
        self.activo = true;
        self.semaforo_espera_activa=false;
        // Un nuevo ciclo invalida cualquier bucle de control previo (evita duplicados con start() repetidos)
        self.ciclo_control++;
        if (self.ProcesoTimeout) {
            clearTimeout(self.ProcesoTimeout);
            self.ProcesoTimeout=null;
        }
        self.emit('start');
        self.control_time(self.ciclo_control);
    }

    // Detiene el procesamiento de la cola
    this.stop = function () {
        self.activo = false;
        self.ciclo_control++;
        if (self.ProcesoTimeout) {
            clearTimeout(self.ProcesoTimeout);
            self.ProcesoTimeout=null;
        }    
        self.emit('stop');
    }    

    // Para la cola y espera a que no queden procesos lanzados
    this.end = async function () {
        self.stop();
        while (self.procesos_actuales>0) {
           await self.sleep(1000);
        }
        self.emit('end');
    }

    // Borrado de las colas 
    this.reset = async function () {
        
        try {
            await util.promisify(self.db_redis.DEL).bind(self.db_redis)(self.nombre_cola);     
            await util.promisify(self.db_redis.DEL).bind(self.db_redis)(self.nombre_cola_lenta);     
            await util.promisify(self.db_redis.DEL).bind(self.db_redis)(self.nombre_cola_rapida);     
            await util.promisify(self.db_redis.DEL).bind(self.db_redis)(self.reintento_nombre_cola);     
        } catch (error) {        
            self.emit('error',{
                'codigo':8,
                'mensaje':'Error en el Borrdo de las Colas',
                'origen':error                                                   
            });             
            return false;
        }    
        return true;        
    }

    // Numero de Procesos en Ejecuciuón
    this.lanzados =  function  () {
        return self.procesos_actuales;
    }

    // Numero de elementos en Cola (Total)
    this.en_cola =  async function  () {
        let total = 0;
        total += await self.en_espera();
        total += await self.en_reintento();
        return total;
    }


    

    // Obsoleto: se mantiene por compatibilidad, usar en_espera()
    this.en_espera_old =  async function  () {
        return self.en_espera();
    }
 
    // Numero de elementos en Espera Lentos
    this.en_espera =  async function (tipo=null) {
        let nombre_cola = null;
        let paquetes = 0        

        // Normales
        if (tipo=='N') {
            nombre_cola = self.nombre_cola; 
        }
        // Lentas
        else if (tipo=='L') {
            nombre_cola = self.nombre_cola_lenta; 
        }
        // Rapidas
        else if (tipo=='R') {
            nombre_cola = self.nombre_cola_rapida; 
        }   
        // Todas
        else {
            let total = 0;
            total += await self.en_espera('L');
            total += await self.en_espera('N');
            total += await self.en_espera('R');
            return total;
        }

        try {            
            paquetes = await util.promisify(self.db_redis.LLEN).bind(self.db_redis)(nombre_cola); 
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