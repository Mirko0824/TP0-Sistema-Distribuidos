import socket
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet
from threading import Thread, Lock, Event
from protocol.server_protocol import ServerProtocol

class Server:
    def __init__(self, server_host: str, server_port: int, agencyQuorumMin: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.agencyQuorumMin = agencyQuorumMin
        # Inicializo la instancia de lottery
        self.lottery = Lottery("all-bets.csv")
        self.lock = Lock() # Inicializo la instancia de lock
        self.agencies = [] # Inicializo la lista de threads
        self.agenciesCompleted = 0 # Inicializo contador de agencias que completaron el envio
        self.agenciesQuorum = Event() # Inicializo el semaforo para para avisar que se cumplio el quorum

    def _handle_bets(self, clientMessage):
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
        # Uso lock aca para proteger de este metodo que es de uso compartido
        with self.lock:
            self.lottery.store_bets(bets_list)

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

                    self._handle_bets(clientMessage)
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

            # Una vez que se cumple el quorum, obtengo cada ganador y armo el mensaje
            for bet in self.lottery.load_bets():
                # Si el apostador no gano o no pertenece a la agencia actual, lo salteo
                if (not self.lottery.has_won(bet)) or (bet.agency_id != currentAgencyId):
                    continue

                # Concateno todos los datos del ganador en un string separado por comas
                winnerBet = bet.first_name + "," + bet.last_name + "," + str(bet.document) + "," + bet.birthdate + "," + str(bet.number)
                
                # Envio el codigo de operacion de winner al cliente
                if protocol.sendCode("WINNER", message_amount) is None:
                    return None
                message_amount += 1

                # Recibo el codigo de operacion de ack de parte del cliente
                if protocol.receiveOperationCode(message_amount) is None:
                    return None
                message_amount += 1

                # Envio el mensaje de ganador
                if protocol.sendMessage(winnerBet, message_amount) is None:
                    return None
                # Sumo 2 porque se envia header y mensaje
                message_amount += 2

            if protocol.sendCode("EOF", message_amount) is None:
                return None
            message_amount += 1

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e
        finally:
            # Aseguro que siempre se cierre el socket
            client_socket.close()

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                    # Creo un thread para cada conexion nueva que llega
                    # Paso por parametro que funcion tiene que ejecutar el thread y los argumento
                    newConnection = Thread(target=self._handle_client, args=(client_socket,))
                    newConnection.start()
                    # Agrego el thread a la lista de threads
                    self.agencies.append(newConnection)
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)