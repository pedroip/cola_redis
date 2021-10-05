# cola_redis

Cola Redis, no permite crear colas en redis de una forma rápida y sencilla.

La cola se creará sobre sobre una conexión redis con un nombre dado, datos que se pasarán como parámetro.

La colas creadas con Cola Redis, tendran las siguientes características:

Compartida con múltiples instancias de  una aplicación.
Regular el número de llamadas concurrentes de cada instancia.
Regular el flujo/ritmo de extracción de la cola en (procesos/segundo) en cada instancia
Control el número de reintentos de un proceso de la cola
