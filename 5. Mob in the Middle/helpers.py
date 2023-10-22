import logging
import socket


def recv_data(conn: socket.socket):
    request, size = "", 2
    while True:
        message = conn.recv(size)
        request += message.decode()
        if not message:
            logging.debug("Client disconnected.")
            # conn.close()
            return request
        if request.endswith("\n"):
            logging.debug(f"Request : {request.strip()}")
            return request


def send_data(conn: socket.socket, response: str):
    try:
        conn.send(response.encode())
        logging.info(f"Response : {response.strip()}")
        # logging.info(f"Sent {len(response)} bytes.")
    except Exception as E:
        logging.error(E)
