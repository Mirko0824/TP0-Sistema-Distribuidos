Redactar un breve informe en donde se detallen los aspectos más importantes de la solución provista, como ser el protocolo de comunicación implementado y los mecanismos para sincronizar la ejecución concurrente.

## Protocolo de Comunicación

Se implementó un protocolo de comunicación basado en **códigos de operación** definidos en hexadecimal (`0x01` para BATCH, `0x02` para EOF, `0x03` para ACK, `0x04` para WINNER) que ocupa **1 byte**, se envía de forma individual antes de cada mensaje para indicar el tipo de mensaje a enviar. Cada vez que se envia un mensaje se espera recibir un mensaje de `ACK` para confirmar que se recibió correctamente. 

Los mensajes de batch o winners están compuestos por un **header de 4 bytes** que indica la cantidad de bytes del mensaje serializado a big endian y luego este se concatena con el mensaje principal encodeado a utf-8. Tanto el servidor como el cliente, esperan recibir primero el codigo de operación de un byte y después un mensaje extenso donde se chequea los primeros 4 bytes para que el socket sepa exactamente cuantos bytes debe recibir.

## Manejo de Concurrencia y Sincronización

En cuanto a la implementación del manejo de threads para la gestión de las múltiples conexiones concurrentes, primero el servidor crea una nueva instancia de thread para cada conexión entrante del cliente. 

### Global Interpreter Lock (GIL)

Teniendo en cuenta que CPython utiliza un **Global Interpreter Lock (GIL)** que restringe la ejecución de código Python a un solo thread a la vez, esto no afecta a la concurrencia implementada. Porque los threads pasan la mayor parte del tiempo esperando operaciones de red (recibir o enviar datos a través del socket) o realizando operaciones de lectura o escritura sobre recursos compartidos. Cuando un thread realiza una operación de Input u Output bloqueante, automáticamente libera el GIL, permitiendo que el intérprete asigne tiempo de CPU a otros threads que estén listos para ejecutarse. Por lo tanto, el GIL no representa un cuello de botella en este contexto.

### Read/Write Lock con Condition Variable

Como todos los threads tienen que acceder al único archivo que guarda todas las apuestas, se implementó un Read y Write Lock usando la clase `Condition` de la librería `threading` que requiere el cumplimiento de ciertas condiciones para funcionar correctamente. 

Esta condition variable encapsula un **Lock** para proteger el acceso del estado compartido y además tiene una **cola de espera**. Cuando un thread quiere acceder al archivo para leer, verifíca que no haya ningún thread escribiendo ni esperando para escribir. Si es así, adquiere el lock de lectura y se incrementa el contador de threads que están leyendo. Por el contrario, cuando un thread quiere escribir, tiene que esperar a que no haya absolutamente ningún thread leyendo ni escribiendo. Si el recurso está ocupado, llama a `wait()`, lo cual hace que se vaya a dormir a la cola de espera, evitando el consumo de procesador (*busy-waiting*). Una vez que el thread que estaba usando el archivo termina con su tarea, libera el lock y hace un `notify_all()`, despertando a todos los threads que estaban en la cola de espera para que vuelvan a poder adquirir el recurso.

Cuando los threads se despiertan, no continúan su ejecución inmediatamente. Dado que el `Condition` encapsula un Lock único, todos los threads que se despiertan compiten para readquirir el Lock antes de poder avanzar. La "cola de espera" **no asegura un orden estricto de despertar** (no es puramente FIFO), sino que la asignación del lock es no determinística y depende del planificador (*scheduler*) del sistema operativo. Solo uno de ellos lo obtiene a la vez, mientras que el resto se queda bloqueado esperando. Por eso, el thread ganador, al adquirir el Lock, reevalúa la condición lógica gracias a la iteración del `while` para asegurarse que puede leer o escribir. Si otro thread le ganó de mano adquiriendo el lock y modificó los valores de las condiciones de lectura o escritura, y por lo tanto el recurso vuelve a estar ocupado, el thread perdedor nuevamente llama a `wait()`.

### Gestión del Quorum con Event

Por otro lado, se utilizó un `Event` de la librería `threading` para gestionar el quorum. Como ningún thread puede arrancar a leer y enviar los ganadores hasta que cierta cantidad mínima de agencias hayan terminado de enviar sus datos, los threads usan el método `wait()` del Event para quedarse dormidos apenas terminan de guardar todas sus apuestas. Recién cuando el servidor registra que llegó la cantidad mínima de agencias requeridas, invoca el método `set()` del Event. Esto actúa como un **semáforo en verde** despertando y dejando pasar en simultáneo a todos los threads dormidos para que procedan a la fase de lectura de resultados. Esto tambien permite que cualquier thread que llegue mas tarde, sepa que ya pasó la fase de recepción de datos y pueda ir directamente a leer los ganadores sin tener que dormirse.

## Manejo de Cierre Controlado (Graceful Shutdown)

En el manejo de la señal de `SIGTERM`, para evitar que los threads se queden bloqueados indefinidamente (`zombies`), el hilo principal primero cambia el valor del flag `self.active` a `False` para indicar que el servidor debe detenerse, y luego se consideran tres escenarios puntuales para destrabar a los threads:

1. Se llama `set()` para despertar a todos los threads que estan esperando que se cumpla el quorum. Al salir del bloqueo, evalúan la condición de `self.active` y finalizan su ejecución.
2. Se llama `notify_all()` para despertar a todos los threads que esten esperando en la cola del lock de lectura/escritura. Al salir del bloqueo, evalúan la condición de `self.active` y finalizan su ejecución.
3. Se itera por cada conexión activa de `client_socket` y se cierra con `close()`. Abortando de esta manera cualquier operacion que se estuviera realizando de recepción o envío liberando al thread de la espera bloqueante.