Cola Redis, es un pequeño objeto que nos permite crear colas en redis de una forma rápida y sencilla.

La cola se creará sobre sobre una conexión redis existente en el proyecto o creada expresamente, un nombre de cola que se usará como base para crear las LIST en redis y la función síncrona que realizará procesado de los datos de la cola, estos datos se pasan como parámetro al instanciar el objeto.

La colas creadas con Cola Redis, tendrán las siguientes características:

Al estar en REDIS puede ser compartida con múltiples instancias y aplicaciones en diferentes equipos (Multiproceso).
Los datos de la cola serán persistentes, no se borraran ni al iniciar ni al finalizar la instancia.
Permite regular/limitar el número de llamadas concurrentes de cada instancia para evitar saturaciones.
Permite regular el velocidad/ritmo de extracción de los elementos de la cola en (procesos/segundo) en cada instancia
Contempla un control de reintentos de un proceso de la cola en caso de fallo.
La extracción de información de la cola se hará de forma ordenada con independencia del tiempo de finalización que dependera del tiempo de ejecución.

*Nota: Para el correcto funcionamiento del procesado en diferentes equipos, es fundamental la sincronización de la hora entre ellos.*

# Instanciar el objeto:

    const db_redis = redis.createClient('redis://xxx.xxx.xxx.xxx:6379?db=n');
    var envios = new cola_redis('pruebas_cola',db_redis,async function(data) {
        let aleatorio = Math.random();             
        if (aleatorio<0.3) {            
            if (aleatorio<0.1) {
                throw {'no_reintentar':true,error:'Error Aleatorio. No Reintentar Orden: '+data.orden};
            } 
            throw 'Error Aleatorio. Reintentar Orden: '+data.orden;            
        }
        else {            
            return 'Procesado Correctamente Orden: '+data.orden;        
        }             
    });

El procesado de la data de la cola, será realizado por una función asíncrona que tras hacer las operaciones oportuna retornará un valor en caso de que la ejecución sea correcta o una excepción (throw) en caso de error. 

En ambos caso el procesado continuara en Cola Redis, ejecutando un evento en función de la situación   ‘procesado’ , ‘reintento’, ‘rechazado’ .

En la invocación a estos eventos ira toda la información necesaria para tomar las acciones oportunas, delegando estas acciones en el procesado de los eventos y dejar lo mas limpia posible la función de procesamiento.

En el caso de no querer hacer reintentos tras la invocación de *throw*, este debera enviar un objeto con la propiedad no_reintentar a true y otra propiedad con el error a inclier el la llamada al evento oportuno.  Ejemplo:  throw {no_reintentar:true,error:error_original};




# Valores de creación
 
    new cola_redis(<nombre_lista_redis>,<objeto redis>,db_redis,async function(data) {<procesado de la data>});

Los valores iniciales Cola Redis son:

    procesos_maximos = 5; // Número máximo de procesos concurrente por instancia  
    reintento_segundos = 120; // Segundos entre reintentos  del procesado
    reintento_numero = 6; // Número por defecto de reintentos, se puede personalizar en cada entrda
    envios_max_segundo = 0; // Número máximo de envíos por segundo en la instancia con independencia del número de procesos máximos. El valor 0 indica que no hay limitación.

# Variables de parametrización. 

Todos los valores anteriormente son susceptibles de cambios durante la ejecución alterando la operativa en el instante del cambio.

# Propiedades del objeto

**add(data_a_procesar,[intentos_maximos])** : Añade un dato/valor  a la cola de procesamiento y opcionalmente se puede indicar cual es el numero maximo de intentos para esos datos.

**start()**: inicia/reanuda el procesado de la cola.
**stopt()**: para el procesado de la cola.
**end()**: para el proceso de la cola y finaliza todos los procesos lanzados.
**reset()**: Borrar todos los elementos de la cola de espera y reintentos.

**lanzados()**: nos dice el número de procesos de la cola  actualmente en ejecución en la instancia.

**en_espera()**: función síncrona/promesa que nos retorna el número de elementos en la cola a la espera de ser procesados.
	
**en_reintento()**: función síncrona/promesa que nos retorna el número de elementos en la cola de reintentos.
	
**en_cola()**: función síncrona/promesa que nos retorna la suma de los elementos en la cola de espera y reintentos.
	
**ultimo()**: nos retorna el timestamp del último procesado de la instancia.

Ejemplos: 

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

# eventos 

**start**: Se ha iniciado/reiniciado el procesado de la cola.
	
**stop**: Se ha detenido el procesado de la cola
	
**end**: Se finalizará la solicitud de end.

**procesado**: Se informa que se ha completado el procesado de un elemento de la cola de forma correcta.  Se acompaña del objeto con la siguiente información.
    - codigo: 0 -> Ejecución correcta
    - mensaje: Información del evento en formato texto.
    - respuesta: información retornada por la función de procesado asociada a la instancia. 
    - intentos: número de intentos utilizados para llegar al éxito.
    - espera_ms: tiempo en milisegundos que ha permanecido en la cola la información antes de ser procesada.
    - data: información original de la cola que fue procesada.

**reintento**: Se ha registrado un error de proceso y se intentará más tarde
    - codigo: 7 -> Error en el procesado y enviado a la cola de reintentos
    - mensaje: Información del evento en formato texto.
    - origen: error reportado por el procesador de la cola.. 
    - intentos: número de intentos acumulados
    - data: información original de la cola que fue procesada.

**rechazado**: Se ha registrado un error de proceso y no se reintentara más.
    - codigo: 6/9 -> Error en el procesado y alcanzo el maximo de intentos
    - mensaje: Información del evento en formato texto.
    - origen: error reportado por el procesador de la cola.. 
    - intentos: número de intentos realizados.
    - data: información original de la cola que fue procesada.

**error**: Se ha detectado un error en la instancia de cola redis.
	- codigo:  Código de error producido.
	- mensaje: Información del evento en formato texto.
	- origen: error reportado por el procesador de la cola.. 
	- intentos: (opcional)  número de intentos
	- data: (opcional)  información original de la cola.

Ejemplo:
	
    envios.on('procesado',function(proceso) {    
        console.log('Evento de PROCESADO Orden:'+proceso.data.orden+' en '+proceso.intentos+' intentos. '+new Date());
    })


# Códigos de Error:
	
	1 - Error Push Cola Espera
	2 - Error Push Reintento
	3 - Error Push Retorno de Reintento
	4 - Error Call Procesado de Data
	5 - Error Chequeo de Cola
	6 - Error en el procesado y alcanzo el maximo de intentos
	7 - Error en el procesado, se reintentará el procesado
	8 - Error en el Borrdo de las Colas
    9 - Error en el procesado y no se reintentara
