import socket
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet
from threading import Thread, Lock, Event, Condition
from protocol.server_protocol import ServerProtocol

class Server:
    def __init__(self, server_host: str, server_port: int, agencyQuorumMin: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agencyQuorumMin = agencyQuorumMin
        self.lottery = Lottery("all-bets.csv") # Inicializo la instancia de lottery
        self.lock = Lock() # Inicializo la instancia de lock
        self.rwCondition = Condition(self.lock) # Creo la condicion para el lock
        self.readers = 0 # Inicio contador de lectores
        self.writersWaiting = 0 # Inicio contador de escritores esperando
        self.writersActive = False # Flag para indicar si hay un escritor activo
        self.agencies = [] # Inicializo la lista de threads
        self.client_sockets = [] # Lista de sockets de clientes activos
        self.agenciesCompleted = 0 # Inicializo contador de agencias que completaron el envio
        self.agenciesQuorum = Event() # Inicializo el semaforo para para avisar que se cumplio el quorum
        self.active = True

    def lockRead(self):
        # Adquiero el lock de lectura
        with self.rwCondition:
            # Mientras exista un thread escribiendo o esperando para escribir
            # Entonces llamo a wait para que suelte el lock y se duerma
            while self.writersActive or self.writersWaiting > 0:
                # Si se corta la ejecucion del programa, salgo
                if not self.active:
                    # Devuelvo false para que entre en el finally del lock y libere el lock
                    return False
                self.rwCondition.wait()
            # Puedo leer porque no hay nadie escribiendo ni esperando para escribir
            # Incremento el contador de lectores
            self.readers += 1
            return True

    def unlockRead(self):
        # Adquiero el lock de lectura
        with self.rwCondition:
            # Decremento el contador de lectores
            self.readers -= 1
            # Si no hay nadie leyendo
            if self.readers == 0:
                # Despierto a todos los threads que esten esperando el quorum
                self.rwCondition.notify_all()

    def lockWrite(self):
        # Adquiero el lock de lectura
        with self.rwCondition:
            # Incremento el contador de threads esperando para escribir
            self.writersWaiting += 1
            # Mientras existan threads leyendo o un thread escribiendo
            while self.readers > 0 or self.writersActive:
                # Si se corta la ejecucion del programa, salgo
                if not self.active:
                    # Devuelvo false para que entre en el finally del lock y libere el lock
                    self.writersWaiting -= 1
                    return False
                self.rwCondition.wait()
            # Disminuyo el contador de threads esperando para escribir
            self.writersWaiting -= 1
            # El thread que esta escribiendo
            self.writersActive = True
            return True

    def unlockWrite(self):
        # Adquiero el lock de lectura
        with self.rwCondition:
            # El thread termino de escribir
            self.writersActive = False
            # Despierto a todos los threads que esten esperando en los locks de lectura/escritura
            self.rwCondition.notify_all()
    
    def stopServer(self):
        self.active = False
        
        # Desbloqueo a todos los threads que esten esperando el quorum
        self.agenciesQuorum.set()

        # Desbloqueo a todos los threads que esten esperando en los locks de lectura/escritura
        with self.rwCondition:
            self.rwCondition.notify_all()
        
        # Cierro todas las conexiones con los clientes para abortar inmediatamente cualquier lectura/escritura
        for sock in self.client_sockets:
            try:
                sock.close()
            except OSError:
                pass

        if hasattr(self, 'server_socket'):
            self.server_socket.close()
            logger.info("server-shutdown", logger.LogResult.success)
    
    def _stopThreads(self):
        # Espero a que todos los threads terminen su ejecucion
        for t in self.agencies:
            logger.info("joining thread", logger.LogResult.in_progress)
            t.join()
            logger.info("joined thread", logger.LogResult.success)

    def _handleBets(self, clientMessage):
        messageBatch = clientMessage.split("\n")
        # Creo el objeto bet con los datos recibidos del cliente
        bets_list = []
        for b in messageBatch:
            # Convierto en lista cada row
            b_row = b.split(",")
            # Creo el objeto bet con los datos recibidos del cliente
            bet = Bet(
                agency_id=int(b_row[0]),
                first_name=b_row[1],
                last_name=b_row[2],
                document=int(b_row[3]),
                birthdate=b_row[4],
                number=int(b_row[5])
            )
            # Agrego el bet a la lista
            bets_list.append(bet)
        # Guardo todas la lista de bets recibidas
        # Uso lockWrite aca para proteger acceso de escritura al archivo
        if not self.lockWrite():
            return
        self.lottery.store_bets(bets_list)
        self.unlockWrite()

    def _handle_client(self, client_socket):
        # Inicializo la instancia del protocolo de servidor
        protocol = ServerProtocol(client_socket)

        action = "handle-client"
        message_amount = 0
        currentAgencyId = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                # Recibo el codigo de la operacion
                operationCode = protocol.receiveOperationCode(message_amount)
                if operationCode is None:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    break
                message_amount += 1
                
                # Despues de haber recibido el codigo de operacion, envio un ack al cliente notificando que lo recibi bien
                if protocol.sendCode("ACK", message_amount) is None:
                    logger.error("send-ack", logger.LogResult.fail, "agency-id", currentAgencyId, "message-id", message_amount)
                    return None
                message_amount += 1

                # Verifico si recibi un codigo de eof o batch
                if operationCode == "EOF":
                    logger.info("finish-receiving-messages", logger.LogResult.success, "messages-amount", message_amount)
                    break
                
                if operationCode == "BATCH":
                    clientMessage = protocol.receiveMessage(message_amount)
                    if clientMessage is None:
                        logger.error("receive-message", logger.LogResult.fail, "agency-id", currentAgencyId, "message-id", message_amount)
                        return None
                    message_amount += 1

                    # Verifico si el agencyId no esta asignado
                    if currentAgencyId is None:
                        # Obtengo la primera linea
                        firstLine = clientMessage.split("\n")[0]
                        # Obtengo el agencyId de la primera linea
                        currentAgencyId = int(firstLine.split(",")[0])
                        # Paso el agencyId al protocolo
                        protocol.setAgencyId(currentAgencyId)

                    self._handleBets(clientMessage)
                    # Envio ack al cliente por haber completado el procesamiento del batch
                    if protocol.sendCode("ACK", message_amount) is None:
                        return None
                    message_amount += 1
            
            # Despues de recibir todo del cliente, pido el lock para incrementar la cantidad de agencias que completaron
            # Verifico que se cumplio el quorum para comenzar a enviar los ganadores
            with self.lock:
                self.agenciesCompleted += 1
                if self.agenciesCompleted == self.agencyQuorumMin:
                    # Si se cumple el quorum, avisa a todos los threads que estan esperando
                    self.agenciesQuorum.set()

            # Se duerme el thread hasta que se cumpla el quorum
            self.agenciesQuorum.wait()

            # Si el thread es despertado y el servidor se esta apagando, sale
            if not self.active:
                return None

            # Una vez que se cumple el quorum, obtengo cada ganador y armo el mensaje
            if not self.lockRead():
                return None
            for bet in self.lottery.load_bets():
                # Si el apostador no gano o no pertenece a la agencia actual, lo salteo
                if (not self.lottery.has_won(bet)) or (bet.agency_id != currentAgencyId):
                    continue

                # Concateno todos los datos del ganador en un string separado por comas
                winnerBet = bet.first_name + "," + bet.last_name + "," + str(bet.document) + "," + bet.birthdate + "," + str(bet.number)
                
                # Envio el codigo de operacion de winner al cliente
                if protocol.sendCode("WINNER", message_amount) is None:
                    self.unlockRead()
                    return None
                message_amount += 1

                # Recibo el codigo de operacion de ack de parte del cliente
                if protocol.receiveOperationCode(message_amount) is None:
                    self.unlockRead()
                    return None
                message_amount += 1

                # Envio el mensaje de ganador
                if protocol.sendMessage(winnerBet, message_amount) is None:
                    self.unlockRead()
                    return None
                # Sumo 2 porque se envia header y mensaje
                message_amount += 2

            self.unlockRead()

            if protocol.sendCode("EOF", message_amount) is None:
                return None
            message_amount += 1

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            # Elimino el socket de la lista
            self.client_sockets.remove(client_socket)
            # Cierro el socket
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            # Guardo el socket para poder cerrarlo desde el stopServer
            self.server_socket = server_socket

            while self.active:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = self.server_socket.accept()
                    # Creo un thread para cada conexion nueva que llega
                    # Paso por parametro que funcion tiene que ejecutar el thread y los argumento
                    newConnection = Thread(target=self._handle_client, args=(client_socket,))
                    newConnection.start()
                    # Agrego el thread a la lista de threads
                    self.agencies.append(newConnection)
                    # Agrego el socket a la lista de sockets activos
                    self.client_sockets.append(client_socket)
                except OSError:
                    if not self.active:
                        break
                    logger.error(action, logger.LogResult.fail)
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

            # Una vez que se cierra el socket, espero a que todos los threads terminen su ejecucion
            self._stopThreads()