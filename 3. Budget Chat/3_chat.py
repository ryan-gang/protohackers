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
    message = f"* The room contains: {participants}\n"
    send_and_log(conn, message, client)


def broadcast_to_all_except(text: str, excluded_client: Optional[str], chat_mode=False) -> None:
    for name in CLIENTS:
        if name != excluded_client:
            conn = CLIENTS[name]
            if chat_mode:
                response = f"[{excluded_client}] {text}\n"
            else:
                response = text
            send_and_log(conn, response, name)


def process_new_client(name: str, conn: socket.socket):
    CLIENTS[name] = conn
    client_name = name
    joining_message = f"* {name} has entered the room\n"
    broadcast_to_all_except(joining_message, client_name)

    # if len(CLIENTS.keys()) > 1:
    broadcast_room_details(client_name)


def process_exiting_client(name: Optional[str], conn: socket.socket, addr):
    logging.debug(f"Client disconnected : {addr}")
    if name in CLIENTS:
        del CLIENTS[name]
    conn.close()
    exiting_message = f"* {name} has left the room\n"
    broadcast_to_all_except(exiting_message, name)


def send_and_log(conn: socket.socket, response: str, client_name: str):
    conn.send(response.encode())
    logging.info(f"Response : {response}")
    logging.info(f"Sent {len(response)} bytes to {client_name}")


def handler(conn: socket.socket, addr: socket.AddressFamily):
    # Handler serves every connection, until it's closed.
    client_name: Optional[str] = None
    size = 1024

    logging.info(f"Connected by : {addr}")
    while True:
        if not client_name:
            response = "Welcome to budgetchat! What shall I call you?\n"
            send_and_log(conn, response, "new_client")
        message = conn.recv(size)
        logging.debug(message)
        if not message.strip():
            # Client disconnected
            process_exiting_client(client_name, conn, addr)
            break

        if not client_name:
            name = message.decode().strip()
            if valid_name(name):
                process_new_client(name, conn)
                client_name = name
            else:
                process_exiting_client(client_name, conn, addr)
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
