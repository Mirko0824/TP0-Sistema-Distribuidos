package client

import (
	"net"
	"time"
	"os"
	"bufio"
	"bytes"

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
	active     bool
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}
	// Inicializo la instancia de protocolo
	clientProtocol := protocol.NewClientProtocol(conn, config.AgencyId)

	client := &Client{conn: conn, config: config, protocol: clientProtocol, active: true}
	return client, nil
}

func (client *Client) Stop() {
	client.active = false
	if client.conn != nil {
		// Cierro la conn del cliente
		client.conn.Close()
	}
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

func handleReadFile(client *Client, scanner *bufio.Scanner, agencyId string, buf *bytes.Buffer) (bool, error) {
	// Reseteo el buffer
	buf.Reset()
	linesRead := 0
	// Leo la cantidad de linea que defina BatchSize
	for i := 0; i < client.config.BatchSize; i++ {
		// Leo la siguiente linea
		if !scanner.Scan() {
			err := scanner.Err()
			if err != nil {
				logger.Error("read-input-file", logger.Fail, "err", err)
				return false, err
			}
			// Si no hay error pero scan devuelve false
			// llegue al final del archivo, salgo
			break
		}

		// Despues de leer la primera linea, si el buffer no tiene capacidad suficiente
		// se calcula en base a los bytes de la linea + agencyId + coma + salto de linea
		// y reservo memoria multiplicando por el BatchSize para evitar reallocaciones de memoria
		if linesRead == 0 && buf.Cap() == 0 {
			estimatedLineSize := len(scanner.Bytes()) + len(agencyId) + 2
			buf.Grow(estimatedLineSize * client.config.BatchSize)
		}

		// Empiezo a agregar \n despues de la primera linea
		// Separo cada linea con \n
		if linesRead > 0 {
			buf.WriteByte('\n')
		}
		// Escribo el agencyId y la coma en el buff
		buf.WriteString(agencyId)
		buf.WriteByte(',')
		// Escribo la linea en bytes
		buf.Write(scanner.Bytes())
		linesRead++
	}
	// Devuelvo true si leyo por lo menos una linea
	return linesRead > 0, nil
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

	// Uso un bufio.Writer para el archivo de salida
	outputWriter := bufio.NewWriter(outputFile)
	defer outputWriter.Flush()

	// Declaro un scanner para leer el archivo de input
	scanner := bufio.NewScanner(inputFile)

	// Inicializo un contador de mensajes
	messageId := 0

	// Inicializo un buffer de bytes para el batch
	var batchBuffer bytes.buffer
	
	// Inicio bucle para leer linea por linea del csv y envio al servidor
	for client.active {
		// Leo el archivo usando el scanner y el buffer
		hasLines, err := handleReadFile(client, scanner, client.config.AgencyId, &batchBuffer)
		if err != nil {
			return err
		}
		if !hasLines {
			break
		}

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

		// Envio el batch, pasando directamente los bytes del buffer
		// Hago batchBuffer.Bytes() para convertilo en un slice de bytes
		if err := client.protocol.SendMessage(batchBuffer.Bytes(), messageId); err != nil {
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

	for client.active {
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

		// Escribo el mensaje de output directamente en el writer bufferizado
		if _, err := outputWriter.Write(receivedResponse); err != nil {
			logger.Error("write-output-file", logger.Fail, "err", err)
			return err
		}
		// Escribo el salto de linea (sin crear slice de bytes nuevo)
		if err := outputWriter.WriteByte('\n'); err != nil {
			logger.Error("write-output-file", logger.Fail, "err", err)
			return err
		}

	}
	logger.Info("all-messages-sent-and-received", logger.Success, "agency-id", client.config.AgencyId, "messages-amount", messageId)

	return nil
}
