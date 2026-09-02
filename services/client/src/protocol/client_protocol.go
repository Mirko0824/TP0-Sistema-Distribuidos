package protocol

import (
	"net"
	"encoding/binary"
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
	conn net.Conn
	agencyId string
}

func NewClientProtocol(conn net.Conn, agencyId string) *ClientProtocol {
	return &ClientProtocol{conn: conn, agencyId: agencyId}
}

func (protocol *ClientProtocol) encodeMessageSize(messageSize uint32) []byte {
	// Creo el header con el size del mensaje
	headerMessage := make([]byte, headerSize)
	// Lo serializo a big endian y lo guardo en el headerMessage
	binary.BigEndian.PutUint32(headerMessage, messageSize)
	return headerMessage
}

func (protocol *ClientProtocol) handleSend(socket net.Conn, message []byte, messageId int) error {
	if err := safe_socket.SendAll(socket, message); err != nil {
		logger.Error("handle-send", logger.Fail, "agency-id", protocol.agencyId, "message-id", messageId)
		return err
	}
	return nil
}

func (protocol *ClientProtocol) handleReceive(socket net.Conn, messageSize int, messageId int) ([]byte, error) {
	receivedMessage, err := safe_socket.RecvAll(socket, messageSize)
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
	codeBytes := []byte{codeMap[code]}
	if err := protocol.handleSend(protocol.conn, codeBytes, messageId); err != nil {
		return err
	}
	return nil
}

func (protocol *ClientProtocol) SendMessage(message string, messageId int) error {
	// Obtengo el len del mensaje y lo convierto a uint32
	totalBytesMessage := uint32(len(message))
	// Serializo el size del mensaje a big endian
	headerBytesSended := protocol.encodeMessageSize(totalBytesMessage)
	// Envio el size del mensaje
	if err := protocol.handleSend(protocol.conn, headerBytesSended, messageId); err != nil {
		return err
	}
	// Convierto el mensaje en string a un slice de bytes
	messageToSend := []byte(message)
	if err := protocol.handleSend(protocol.conn, messageToSend, messageId); err != nil {
		return err
	}
	return nil
}

func (protocol *ClientProtocol) ReceiveOperationCode(messageId int) (string, error) {
	receivedCode, err := protocol.handleReceive(protocol.conn, codeSize, messageId)
	if err != nil {
		return "", err
	}
	// Verifico que codigo recibi
	code := protocol.handleCode(receivedCode[0], messageId)
	return code, nil
}

func (protocol *ClientProtocol) ReceiveMessage(messageId int) (string, error) {
	// Recibo el size del mensaje entrante
	headerResponse, err := protocol.handleReceive(protocol.conn, headerSize, messageId)
	if err != nil {
		return "", err
	}
	// Desencodeo el header para obtener el size del mensaje
	totalBytesToReceive := binary.BigEndian.Uint32(headerResponse)
	receivedResponse, err := protocol.handleReceive(protocol.conn, int(totalBytesToReceive), messageId)
	if err != nil {
		return "", err
	}
	return string(receivedResponse), nil
}
