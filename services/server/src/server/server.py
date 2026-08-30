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

    def _handle_bets(self, client_message):
        # Creo el objeto bet con los datos recibidos del cliente
        bets_list = []
        for b in client_message:
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
        # Guardo todas la lista de apuetas recibidas
        self.lottery.store_bets(bets_list)

    def _send_eof(self, client_socket, agencyId, messageId):
        # Encodeo el mensaje EOF y transformo a big endian para el header
        endMessage = "EOF".encode("utf-8")
        if self._handle_send_message(client_socket, agencyId, messageId, endMessage) is None:
            logger.error("send-eof", logger.LogResult.fail, "agency-id", agencyId, "message-id", messageId)
            return None

        return True
    
    def _send_ack(self, client_socket, agencyId, messageId):
        # Enviar ACK al cliente por haber completado el procesamiento del batch
        ack = "ACK".encode("utf-8")
        if self._handle_send_message(client_socket, agencyId, messageId, ack) is None:
            logger.error("send-ack", logger.LogResult.fail, "agency-id", agencyId, "message-id", messageId)
            return None

        return True

    def _handle_send_message(self, client_socket, agencyId, messageId, message):
        # Obtengo el tamaño del mensaje en bytes
        totalBytesMessage = len(message)
        # Creo el encabezado con el tamaño del mensaje
        headerMessage = totalBytesMessage.to_bytes(HEADER_SIZE, byteorder="big")
        # Envio el header con la cantidad de bytes a enviar
        if safe_socket.send_all(client_socket, headerMessage) is None:
            logger.error("send-message", logger.LogResult.fail, "agency-id", agencyId, "message-id", messageId)
            return None
        # Envio el mensaje
        if safe_socket.send_all(client_socket, message) is None:
            logger.error("send-message", logger.LogResult.fail, "agency-id", agencyId, "message-id", messageId)
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
                    return None
                
                message_amount += 2
                # Decodifico el mensaje recibido a utf-8
                client_message_decoded = client_message.decode("utf-8")

                # Si recibo "EOF", significa que el cliente termino de enviarme todo
                if client_message_decoded == "EOF":
                    logger.info("finish-receiving-messages", logger.LogResult.success, "messages-amount", message_amount)
                    break

                # Separo cada row del csv
                bet_rows = client_message_decoded.split("\n")
                # Guardo el agencyId del cliente actual
                current_agency_id = int(bet_rows[0].split(",")[0])
                self._handle_bets(bet_rows)

                # Envio ACK al cliente por haber completado el procesamiento del batch
                if self._send_ack(client_socket, current_agency_id, message_amount) is None:
                    logger.error("send-ack", logger.LogResult.fail, "agency-id", current_agency_id, "message-id", message_amount)
                    return None
                
                message_amount += 2

            # Obtengo cada ganador y armo el mensaje
            for bet in self.lottery.load_bets():
                # Si el apostador no gano o no pertenece a la agencia actual, lo salto
                if (not self.lottery.has_won(bet)) or (bet.agency_id != current_agency_id):
                    continue

                # Concateno todos los datos del ganador en un string separado por comas
                winnerBet = bet.first_name + "," + bet.last_name + "," + str(bet.document) + "," + bet.birthdate + "," + str(bet.number)
                # Encondeo el mensaje para enviarlo
                betMessage = winnerBet.encode("utf-8")
                # Envio el header y el mensaje
                if self._handle_send_message(client_socket, bet.agency_id, message_amount, betMessage) is None:
                    logger.error("send-response", logger.LogResult.fail, "agency-id", bet.agency_id, "message-id", message_amount)
                    return None

                message_amount += 2

            # Envio el mensaje de fin al cliente, indicando de que termine de enviar todo
            if self._send_eof(client_socket, current_agency_id, message_amount) is None:
                return None
            
            client_socket.close()

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
