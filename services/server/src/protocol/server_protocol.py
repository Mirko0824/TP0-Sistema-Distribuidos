import logger
import safe_socket

class ServerProtocol:

    # Defino los codigos de los mensajes del protocolo de 1 byte
    _CODE_BATCH = 0x01
    _CODE_EOF = 0x02
    _CODE_ACK = 0x03
    _CODE_WINNER = 0x04

    _MESSAGE_SIZE = 4
    _CODE_SIZE = 1

    # Diccionario para mapear strings a codigos reales
    _STR_TO_CODE = {
        "BATCH": _CODE_BATCH,
        "EOF": _CODE_EOF,
        "ACK": _CODE_ACK,
        "WINNER": _CODE_WINNER
    }
    
    def __init__(self, socket, agencyId=None):
        self.socket = socket
        self.agencyId = agencyId

    def _encodeInt(self, value, size):
        # Serializo int a bytes en formato big endian
        return value.to_bytes(size, byteorder="big")

    def _decodeInt(self, bytes_val):
        # Deserializo bytes en formato big endian a int
        return int.from_bytes(bytes_val, byteorder="big")

    def _encodeString(self, text):
        # Codifico un string a bytes
        return text.encode("utf-8")

    def _decodeString(self, bytes_val):
        # Decodifico bytes a un string
        return bytes_val.decode("utf-8")

    def _handleSend(self, socket, message, messageId):
        bytesSended = safe_socket.send_all(socket, message)
        if bytesSended is None:
            logger.error("send-message", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None

        return bytesSended
    
    def _handleReceive(self, socket, messageSize, messageId):
        receivedMessage = safe_socket.recv_all(socket, messageSize)
        if receivedMessage is None:
            logger.error("receive-message", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None
        
        return receivedMessage

    def _handleCode(self, code):
        # Devuelvo el codigo en formato string segun su valor
        if code == self._CODE_BATCH:
            return "BATCH"
        elif code == self._CODE_EOF:
            return "EOF"
        elif code == self._CODE_ACK:
            return "ACK"
        else:
            return None
    
    def setAgencyId(self, agencyId):
        self.agencyId = agencyId

    def sendCode(self, code, messageId=0):
        # Traduzco el codigo de string a int
        codeValue = self._STR_TO_CODE.get(code)
        if codeValue is None:
            logger.error("send-code", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None
            
        # Serializo el codigo a bigendian y lo envio
        codeBytes = self._encodeInt(codeValue, self._CODE_SIZE)
        bytesSended = self._handleSend(self.socket, codeBytes, messageId)
        if bytesSended is None:
            logger.error("send-code", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None
        return bytesSended
    
    def sendMessage(self, message, messageId=0):
        # Codifico el mensaje a bytes
        messageToSend = self._encodeString(message)
        # Serializo el int de la cantidad de bytes del mensaje a bigendian
        headerMessage = self._encodeInt(len(messageToSend), self._MESSAGE_SIZE)
        headerBytesSended = self._handleSend(self.socket, headerMessage, messageId)
        if headerBytesSended is None:
            logger.error("send-header", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None

        # Envio el mensaje principal
        bytesSended = self._handleSend(self.socket, messageToSend, messageId)
        if bytesSended is None:
            logger.error("send-message", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None
        
        # Devuelvo la cantidad de bytes enviados
        return bytesSended

    def receiveOperationCode(self, messageId=0):
        # Recibo el codigo del mensaje de 1 byte
        receivedCode = self._handleReceive(self.socket, self._CODE_SIZE, messageId)
        if receivedCode is None:
            return None
        # Deserializo el codigo recibido
        receivedOperationCode = self._decodeInt(receivedCode)
        
        # Verifico el codigo recibido y lo devuelvo en string
        operationType = self._handleCode(receivedOperationCode)
        if operationType is None:
            logger.error("handle-code", logger.LogResult.fail, "agencyId", self.agencyId, "messageId", messageId)
            return None

        return operationType

    def receiveMessage(self, messageId=0):
        # Recibo el header que me dice cuantos bytes voy a recibir
        receivedHeader = self._handleReceive(self.socket, self._MESSAGE_SIZE, messageId)
        if receivedHeader is None:
            return None
        
        # Deserializo el header para saber cuantos bytes voy a recibir y convierto a int
        totalBytesToReceive = self._decodeInt(receivedHeader)
        
        # Recibo el mensaje indicando la cantidad exacta de bytes que voy a recibir
        receivedMessage = self._handleReceive(self.socket, totalBytesToReceive, messageId)
        if receivedMessage is None:
            return None
        
        # Decodifico el mensaje
        message = self._decodeString(receivedMessage)

        return message