import logging
import socket
import sys
import threading
from typing import Optional
from helpers import valid_name

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    stream=sys.stdout,
)

IP, PORT = "10.138.0.2", 9090
CLIENTS: dict[str, socket.socket] = {}


def broadcast_room_details(client: str) -> None:
    conn = CLIENTS[client]
    names = list(CLIENTS.keys())
    participants = ", ".join(list(filter(lambda x: x != client, names)))
    message = f"* The room contains: {participants}\n".encode()
    conn.send(message)
    logging.info(f"Response : {message}")
    logging.info(f"Sent {len(message)} bytes to {client}")


def broadcast_to_all_except(text: str, excluded_client: str, chat_mode=False) -> None:
    for name in CLIENTS:
        if name != excluded_client:
            conn = CLIENTS[name]
            if chat_mode:
                response = f"[{excluded_client}] {text}\n".encode()
            else:
                response = text.encode()
            conn.send(response)
            logging.info(f"Response : {response}")
            logging.info(f"Sent {len(response)} bytes to {name}")


def handler(conn: socket.socket, addr: socket.AddressFamily):
    # Handler serves every connection, until it's closed.
    client_name: Optional[str] = None
    size = 1024

    logging.info(f"Connected by : {addr}")
    while True:
        if not client_name:
            conn.send("Welcome to budgetchat! What shall I call you?\n".encode())
        message = conn.recv(size)
        logging.debug(message)
        # conn.send(message)
        if not message.strip():
            # Client disconnected
            logging.debug(f"Client disconnected : {addr}")
            conn.close()
            break

        if not client_name:
            name = message.decode().strip()
            if valid_name(name):
                CLIENTS[name] = conn
                client_name = name
                joining_message = f"* {name} has entered the room\n"
                broadcast_room_details(client_name)
                broadcast_to_all_except(joining_message, client_name)
            else:
                conn.close()
        else:
            broadcast_to_all_except(message.decode(), client_name, chat_mode=True)
        logging.debug(f"Request : {message}")


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}:{PORT}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(60)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
