package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	// Inicio el contador de total de bytes enviados
	sentBytes := 0
	// Bucle para enviar todos los bytes
	for sentBytes < len(bytes) {
		// Envio los bytes restantes a partir del indice del ultimo byte enviado
		// n: cantidad de bytes enviados
		n, err := socket.Write(bytes[sentBytes:])
		if err != nil {
			return err
		}
		// Aumento el contador de total de bytes enviados
		sentBytes += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	receiveBuffer := make([]byte, size)
	// Inicio el contador de total de bytes recibidos
	totalBytesReceived := 0
	// Bucle para recibir todos los bytes
	for totalBytesReceived < size {
		// Recibo los bytes restantes a partir del indice del ultimo byte recibido
		// receiveBuffer[totalBytesReceived:]: cantidad de bytes restantes por recibir
		// bytesRead: cantidad de bytes recibidos
		bytesRead, err := socket.Read(receiveBuffer[totalBytesReceived:])
		if err != nil {
			return nil, err
		}
		// Aumento el contador de total de bytes recibidos
		totalBytesReceived += bytesRead
	}
	return receiveBuffer, nil
}
