import socket

# TODO: Complete with a short-read/short-write tolerant implementation


def recv_all(socket: socket.socket, size: int):
    # Inicio contador de cantidad de bytes recibidos
    totalReceivedBytes = 0
    # Creo una variable vacio de tipo bytes para guardar los bytes recibidos
    buffer = b""
    # Bucle para recibir todos los bytes
    while totalReceivedBytes < size:
        # Recibo los bytes restantes a partir del indice del ultimo byte recibido
        # size - receivedBytes: cantidad de bytes restantes por recibir
        receivedBytes = socket.recv(size - totalReceivedBytes)
        # Si no se reciben bytes, devuelvo None
        if not receivedBytes:
            return None
        # Agrego los bytes recibidos al buffer
        buffer += receivedBytes
        # Aumento el contador de total de bytes recibidos
        totalReceivedBytes += len(receivedBytes)
          
    return buffer


def send_all(socket: socket.socket, data: bytes):
    # Inicio contador de cantidad de bytes enviados
    totalSentBytes = 0
    # Bucle para enviar todos los bytes
    while totalSentBytes < len(data):
        # Envio los bytes restantes a partir del indice del ultimo byte enviado
        # data[totalSentBytes:]: cantidad de bytes restantes por enviar
        # sentBytes: cantidad de bytes enviados
        sentBytes = socket.send(data[totalSentBytes:])
        # Si no se pudo realizar el envio, se reintenta
        if sentBytes == 0:
            continue
        # Aumento el contador de total de bytes enviados
        totalSentBytes += sentBytes

    return totalSentBytes
