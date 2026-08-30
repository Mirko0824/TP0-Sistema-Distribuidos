package client

import (
	"net"
	"time"
	"os"
	"encoding/csv"
	"io"
	"strings"
	"encoding/binary"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const HEADER_SIZE = 4
const EOF_MESSAGE = "EOF"
const BATCH_SIZE = 100

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string 
	OutputFile string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func handleOpenFile(client *Client, filePath string, openMode int) (*os.File, error) {
	const action = "open-file"
	logger.Info(action, logger.InProgress, "file", filePath)

	// Abro el archivo accediendo a la ruta guardada en el client.config
	file, err := os.OpenFile(filePath, openMode, 0644)
	if err != nil {
		logger.Error(action, logger.Fail, "err", err)
		return nil, err
	}

	return file, nil
}

func handleReadFile(file *os.File, reader *csv.Reader, agencyId string) ([]string, error) {
	// Creo un array para guardar todas las lineas que leo del csv
	var batch []string
	// Leo la cantidad de linea que defina BATCH_SIZE
	for i := 0; i < BATCH_SIZE; i++ {
		readedTexts, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error("read-input-file", logger.Fail, "err", err)
			return nil, err
		}
		// Agrego el AgencyId delante de la row leida
		batch = append(batch, agencyId + "," + strings.Join(readedTexts, ","))
	}

	return batch, nil
}

func handleSend(client *Client, headerMessage []byte, clientMessage []byte, messageArgs ...any) error {
	// Envio el headerMessage antes que el mensaje principal
	if err := safe_socket.SendAll(client.conn, headerMessage); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}
	// Envio el mensaje principal
	if err := safe_socket.SendAll(client.conn, clientMessage); err != nil {
		logger.Error("send-message", logger.Fail, messageArgs...)
		return err
	}

	return nil
}

func handleReceive(client *Client) ([]byte, error) {
	// Recibo el header de la respuesta
	headerResponse, err := safe_socket.RecvAll(client.conn, HEADER_SIZE)
	if err != nil {
		logger.Error("recv-response", logger.Fail, "err", err, "agency-id", client.config.AgencyId)
		return nil, err
	}
	// Desencodeo el tamaño del mensaje
	totalBytesToReceive := binary.BigEndian.Uint32(headerResponse)

	// Recibo el mensaje
	receivedResponse, err := safe_socket.RecvAll(client.conn, int(totalBytesToReceive))
	if err != nil {
		logger.Error("recv-response", logger.Fail, "err", err, "agency-id", client.config.AgencyId)
		return nil, err
	}

	return receivedResponse, nil
}

func (client *Client) Run() error {
	defer client.conn.Close()

	// Abro el archivo de input a la ruta guardada en el client.config
	inputFile, err := handleOpenFile(client, client.config.InputFile, os.O_RDONLY)
	if err != nil {
		return err
	}
	// Uso defer para cerrar el archivo cuando termine la funcion (RAI)
	defer inputFile.Close()
	
	// Abro el archivo de output a la ruta guardada en el client.config
	outputFile, err := handleOpenFile(client, client.config.OutputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	// Uso defer para cerrar el archivo cuando termine la funcion para que sea RAI
	defer outputFile.Close()

	// Uso un lector de csv estandar
	reader := csv.NewReader(inputFile)
	// El separador de columnas es la coma
	reader.Comma = ','

	// Inicializo un contador de mensajes
	messageId := 0
	var messageArgs []any
	// Inicio bucle para leer linea por linea del csv y envio al servidor
	for {
		// Leo el archivo
		readedBatch, err := handleReadFile(inputFile, reader, client.config.AgencyId)
		if err != nil {
			return err
		}
		if len(readedBatch) == 0 {
			break
		}

		// Sumo 2 porque se envia header + mensaje
		messageId += 2
		messageArgs := []any{"agency-id", client.config.AgencyId, "message-id", messageId}

		// Armo el mensaje separando cada row del csv leido separado con \n
		// Para que del lado del servidor pueda detectar el fin de cada linea
		clientMessage := strings.Join(readedBatch, "\n")
		// Logueo los argumentos del mensaje
		// logger.Info("send-message", logger.Success, "message", clientMessage)

		// Obtengo el tamaño del mensaje en bytes
		totalBytesMessage := uint32(len(clientMessage))
		// Creo el encabezado con el tamaño del mensaje
		headerMessage := make([]byte, HEADER_SIZE)
		// Lo escribo en big endian y lo guardo en el headerMessage
		binary.BigEndian.PutUint32(headerMessage, totalBytesMessage)

		if err := handleSend(client, headerMessage, []byte(clientMessage), messageArgs...); err != nil {
			return err
		}
		// Recibo el ack del server de que proceso correctamente lo que le envie
		ack, err := handleReceive(client)
		if err != nil {
			return err
		}
		if string(ack) != "ACK" {
			logger.Error("recv-ack", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId)
			return err
		}
		logger.Info("recv-ack", logger.Success, "agency-id", client.config.AgencyId, "message-id", messageId)
	}

	// Preparo mensaje de fin
	headerEof := make([]byte, HEADER_SIZE)
	binary.BigEndian.PutUint32(headerEof, uint32(len(EOF_MESSAGE)))
	messageId += 2
	messageArgs = []any{"agency-id", client.config.AgencyId, "message-id", messageId}
	if err := handleSend(client, headerEof, []byte(EOF_MESSAGE), messageArgs...); err != nil {
		return err
	}

	logger.Info("send-finish-message", logger.Success)

	for {
		// Recibo la respuesta del servidor
		receivedResponse, err := handleReceive(client)
		if err != nil {
			return err
		}

		messageId += 2
		messageArgs = []any{"agency-id", client.config.AgencyId, "message-id", messageId}

		// Si el mensaje recibido es EOF, significa que ya recibi todos los mensajes
		if string(receivedResponse) == EOF_MESSAGE {
			logger.Info("finish-receiving-messages", logger.Success, messageArgs...)
			break
		}
		logger.Info("receive-response", logger.Success, "response", string(receivedResponse))

		// Convierto el mensaje recibido a string y le agrego un salto de linea
		outputMessage := string(receivedResponse) + "\n"

		// Escribo el mensaje en el archivo de output
		if _, err := outputFile.WriteString(outputMessage); err != nil {
			logger.Error("write-output-file", logger.Fail, "err", err)
			return err
		}
		logger.Info("write-output-file", logger.Success, "file", client.config.OutputFile)

	}
	logger.Info("all-messages-sent-and-received", logger.Success, "agency-id", client.config.AgencyId, "messages-amount", messageId)

	return nil
}
