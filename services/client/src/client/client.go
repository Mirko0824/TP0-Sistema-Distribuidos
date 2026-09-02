package client

import (
	"net"
	"time"
	"os"
	"bufio"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/protocol"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile  string 
	OutputFile string
	BatchSize  int
}

type Client struct {
	conn       net.Conn
	config     ClientConfig
	protocol   *protocol.ClientProtocol
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	// Inicializo la instancia de protocolo
	clientProtocol := protocol.NewClientProtocol(conn, config.AgencyId)

	client := &Client{conn: conn, config: config, protocol: clientProtocol}
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

func handleReadFile(client *Client, scanner *bufio.Scanner, agencyId string, batch *[]string) error {
	// Reseteo la variable batch
	// Modifico la variable original que recibo por parametro y no una copia
	*batch = (*batch)[:0] 
	// Leo la cantidad de linea que defina BatchSize
	for i := 0; i < client.config.BatchSize; i++ {
		// Leo la siguiente linea
		if !scanner.Scan() {
			err := scanner.Err()
			if err != nil {
				logger.Error("read-input-file", logger.Fail, "err", err)
				return err
			}
			// Si no hay error pero scan devuelve false
			// llegue al final del archivo, salgo
			break
		}
		// Obtengo el texto de la row leida
		row := scanner.Text()
		// Agrego el agencyId delante de la row leida
		*batch = append(*batch, agencyId + "," + row)
	}
	return nil
}

func (client *Client) Run() error {
	defer client.conn.Close()

	// Abro el archivo de input a la ruta guardada en el client.config
	inputFile, err := handleOpenFile(client, client.config.InputFile, os.O_RDONLY)
	if err != nil {
		return err
	}
	// Uso defer para cerrar el archivo cuando termine la funcion
	defer inputFile.Close()
	
	// Abro el archivo de output a la ruta guardada en el client.config
	outputFile, err := handleOpenFile(client, client.config.OutputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	if err != nil {
		return err
	}
	// Uso defer para cerrar el archivo cuando termine la funcion
	defer outputFile.Close()
	// Declaro un scanner para leer el archivo de input
	scanner := bufio.NewScanner(inputFile)

	// Inicializo un contador de mensajes
	messageId := 0

	// Inicializo readedBatch en 0 y con el size del batch
	readedBatch := make([]string, 0, client.config.BatchSize)

	// Inicio bucle para leer linea por linea del csv y envio al servidor
	for {
		// Leo el archivo usando el scanner
		err := handleReadFile(client, scanner, client.config.AgencyId, &readedBatch)
		if err != nil {
			return err
		}
		if len(readedBatch) == 0 {
			break
		}

		// Armo el mensaje separando cada row del csv leido separado con \n
		// Para que del lado del servidor pueda detectar el fin de cada linea
		clientMessage := strings.Join(readedBatch, "\n")

		// Envio el codigo de operacion batch
		if err := client.protocol.SendCode("BATCH", messageId); err != nil {
			return err
		}
		messageId++

		// Espero recibir un ack del server de que recibio correctamente el codigo
		receivedCode, err := client.protocol.ReceiveOperationCode(messageId)
		if err != nil || receivedCode != "ACK" {
			logger.Error("receive-operation-code", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId)
			return err
		}
		messageId++

		// Envio el batch
		if err := client.protocol.SendMessage(clientMessage, messageId); err != nil {
			return err
		}

		// Sumo 2 porque se envia header + mensaje
		messageId += 2

		// Espero recibir el ack del server confirmando que se pudo procesar correctamente el batch enviado
		receivedCode, err = client.protocol.ReceiveOperationCode(messageId)
		if err != nil || receivedCode != "ACK" {
			logger.Error("receive-operation-code", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId)
			return err
		}
		messageId++
	}

	// Envio codigo eof indicando que termino de enviar todo el archivo
	if err := client.protocol.SendCode("EOF", messageId); err != nil {
		return err
	}
	messageId++

	// Espero recibir un ack de que recibio el server bien el eof
	receivedCode, err := client.protocol.ReceiveOperationCode(messageId)
	if err != nil || receivedCode != "ACK" {
		logger.Error("receive-operation-code", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId)
		return err
	}
	messageId++

	logger.Info("send-finish-message", logger.Success)

	for {
		// Recibo el codigo de operacion, espero recibir codigo de winner o eof
		receivedCode, err := client.protocol.ReceiveOperationCode(messageId)
		if err != nil {
			return err
		}
		if receivedCode == "EOF" {
			break
		}
		if receivedCode != "WINNER" {
			logger.Error("receive-operation-code", logger.Fail, "agency-id", client.config.AgencyId, "message-id", messageId)
			return err
		}
		messageId++

		// Envio un ack al server confirmando que recibi el codigo
		if err := client.protocol.SendCode("ACK", messageId); err != nil {
			return err
		}
		messageId++

		// Recibo el mensaje con el ganador
		receivedResponse, err := client.protocol.ReceiveMessage(messageId)
		if err != nil {
			return err
		}
		messageId++

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
