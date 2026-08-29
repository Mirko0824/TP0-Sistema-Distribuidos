import socket
import logger
import safe_socket
from lottery.lottery import Lottery
from lottery.bet import Bet

HEADER_SIZE = 4


class Server:
    def __init__(self, server_host: str, server_port: int) -> None:
        self.server_host = server_host
        self.server_port = server_port
        # Inicializo la instancia de lottery
        self.lottery = Lottery("all_bets.csv")

    def _handle_bet(self, client_message):
        # Creo el objeto bet con los datos recibidos del cliente
        bet = Bet(
            agency_id=int(client_message[0]),
            first_name=client_message[1],
            last_name=client_message[2],
            document=int(client_message[3]),
            birthdate=client_message[4],
            number=int(client_message[5])
        )
        self.lottery.store_bets([bet])

    def _send_eof(self, client_socket, agencyId):
        # Encodeo el mensaje EOF y transformo a big endian para el header
        endMessage = "EOF".encode("utf-8")
        headerFinal = len(endMessage).to_bytes(HEADER_SIZE, byteorder="big")
        # Envio el header con la cantidad de bytes a enviar
        if safe_socket.send_all(client_socket, headerFinal) is None:
            logger.error("send-eof", logger.LogResult.fail, "agency-id", agencyId)
            return None
        # Envio el mensaje
        if safe_socket.send_all(client_socket, endMessage) is None:
            logger.error("send-eof", logger.LogResult.fail, "agency-id", agencyId)
            return None
        return True

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        current_agency_id = None
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                # Recibo el header que me dice cuantos bytes voy a recibir
                headerToReceive = safe_socket.recv_all(client_socket, HEADER_SIZE)
                # Si no recibo nada, salgo del bucle
                if not headerToReceive:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    break
                # Decodifico el header para saber cuantos bytes voy a recibir y convierto a int
                totalBytesToReceive = int.from_bytes(headerToReceive, byteorder="big")

                # Recibo el mensaje indicando la cantidad exacta de bytes que voy a recibir
                client_message = safe_socket.recv_all(client_socket, totalBytesToReceive)
                if not client_message:
                    logger.info(action, logger.LogResult.success, "messages-amount", message_amount)
                    return
                
                message_amount += 2
                
                client_message = client_message.decode("utf-8").split(",")

                # Si recibo "EOF", significa que el cliente termino de enviarme todo
                if client_message[0] == "EOF":
                    logger.info("finish-receiving-messages", logger.LogResult.success, "messages-amount", message_amount)
                    break

                # Guardo el agencyId del cliente actual
                current_agency_id = int(client_message[0])

                # Armo el objeto bet con los datos recibidos
                self._handle_bet(client_message)

            # Obtengo cada ganador y armo el mensaje
            for bet in self.lottery.load_bets():
                # Si el apostador no gano o no pertenece a la agencia actual, lo salto
                if (not self.lottery.has_won(bet)) or (bet.agency_id != current_agency_id):
                    continue

                # Concateno todos los datos del ganador en un string separado por comas
                winnerBet = bet.first_name + "," + bet.last_name + "," + str(bet.document) + "," + bet.birthdate + "," + str(bet.number)
                # Encondeo el mensaje para enviarlo
                betMessage = winnerBet.encode("utf-8")
                # Obtengo el tamaño del mensaje
                totalBytesMessage = len(betMessage)
                # Lo escribo en big endian y lo guardo en el header
                headerResponse = totalBytesMessage.to_bytes(HEADER_SIZE, byteorder="big")
                # Envio el header indicando la cantidad de bytes que va a recibir primero
                if safe_socket.send_all(client_socket, headerResponse) is None:
                    logger.error("send-response", logger.LogResult.fail, "agency-id", bet.agency_id, "message-id", message_amount)
                    return None
                # Envio el mensaje
                if safe_socket.send_all(client_socket, betMessage) is None:
                    logger.error("send-response", logger.LogResult.fail, "agency-id", bet.agency_id, "message-id", message_amount)
                    return None

                message_amount += 2

            # Envio el mensaje de fin al cliente, indicando de que termine de enviar todo
            if self._send_eof(client_socket, bet.agency_id) is None:
                return None

        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
