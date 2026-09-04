package protocol

import (
	"encoding/binary"
	"net"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const (
	// Defino los codigos de los mensajes del protocolo
	codeBatch = 0x01
	codeEof = 0x02
	codeAck = 0x03
	codeWinner = 0x04

	headerSize = 4
	codeSize = 1
)

// Defino mapa para convertir strings a codigos (debe ser var, no const)
var codeMap = map[string]byte{
	"BATCH": codeBatch,
	"EOF": codeEof,
	"ACK": codeAck,
	"WINNER": codeWinner,
}

type ClientProtocol struct {
	conn       net.Conn
	agencyId   string
	headerBuf  []byte
	codeBuf    []byte
	sendBuf    []byte
	receiveBuf []byte
}

func NewClientProtocol(conn net.Conn, agencyId string) *ClientProtocol {
	return &ClientProtocol{
		conn:       conn,
		agencyId:   agencyId,
		headerBuf:  make([]byte, headerSize),
		codeBuf:    make([]byte, codeSize),
	}
}

func (protocol *ClientProtocol) encodeMessageSize(messageSize uint32) []byte {
	// Reutilizo el buffer de la estructura, 0 allocs
	binary.BigEndian.PutUint32(protocol.headerBuf, messageSize)
	return protocol.headerBuf
}

func (protocol *ClientProtocol) handleSend(message []byte, messageId int) error {
	if err := safe_socket.SendAll(protocol.conn, message); err != nil {
		logger.Error("handle-send", logger.Fail, "agency-id", protocol.agencyId, "message-id", messageId)
		return err
	}
	return nil
}

func (protocol *ClientProtocol) handleReceive(messageSize int, messageId int) ([]byte, error) {
	receivedMessage, err := safe_socket.RecvAll(protocol.conn, messageSize)
	if err != nil {
		logger.Error("handle-receive", logger.Fail, "agency-id", protocol.agencyId, "message-id", messageId)
		return nil, err
	}
	return receivedMessage, nil
}

func (protocol *ClientProtocol) handleCode(code byte, messageId int) string {
	switch code {
		case codeBatch:
			return "BATCH"
		case codeEof:
			return "EOF"
		case codeAck:
			return "ACK"
		case codeWinner:
			return "WINNER"
		default:
			return "UNKNOWN"
	}
}

func (protocol *ClientProtocol) SendCode(code string, messageId int) error {
	protocol.codeBuf[0] = codeMap[code] // Reutilizo el buffer de 1 byte
	if err := protocol.handleSend(protocol.codeBuf, messageId); err != nil {
		return err
	}
	return nil
}

func (protocol *ClientProtocol) SendMessage(message []byte, messageId int) error {
	// Obtengo el len del mensaje y lo convierto a uint32
	totalBytesMessage := uint32(len(message))
	// Serializo el size del mensaje a big endian
	headerBytesSended := protocol.encodeMessageSize(totalBytesMessage)

	// Calculo el size total del paquete (header + mensaje)
	totalSize := headerSize + len(message)
	
	// Verifico la capacidad del slice y si no es suficiente hago una reallocation	
	if cap(protocol.sendBuf) < totalSize {
		protocol.sendBuf = make([]byte, totalSize)
	}
	// Tomo solo la porcion de memoria que necesito para este envio
	messageToSend := protocol.sendBuf[:totalSize]
	
	// Copio el header y luego el mensaje dentro del mismo buffer
	// Piso los valores viejos del buffer
	copy(messageToSend[:headerSize], headerBytesSended)
	copy(messageToSend[headerSize:], message)
	
	if err := protocol.handleSend(messageToSend, messageId); err != nil {
		return err
	}
	return nil
}

func (protocol *ClientProtocol) ReceiveOperationCode(messageId int) (string, error) {
	receivedCode, err := protocol.handleReceive(codeSize, messageId)
	if err != nil {
		return "", err
	}
	// Verifico que codigo recibi
	code := protocol.handleCode(receivedCode[0], messageId)
	return code, nil
}

func (protocol *ClientProtocol) ReceiveMessage(messageId int) ([]byte, error) {
	// Recibo el size del mensaje entrante
	headerResponse, err := protocol.handleReceive(headerSize, messageId)
	if err != nil {
		return nil, err
	}
	// Desencodeo el header para obtener el size del mensaje
	totalBytesToReceive := binary.BigEndian.Uint32(headerResponse)

	// Recibo todo el mensaje
	receivedResponse, err := protocol.handleReceive(int(totalBytesToReceive), messageId)
	if err != nil {
		return nil, err
	}
	return receivedResponse, nil
}
