import logging
import socket
import sys
import threading

from helpers import (
    broadcast_to_all_except,
    process_exiting_client,
    process_new_client,
    send_and_log,
    valid_name,
)

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
DEFAULT_CLIENT_NAME = ""


def handler(conn: socket.socket, addr: socket.AddressFamily):
    # Handler serves every connection, until it's closed.
    client_name: str = DEFAULT_CLIENT_NAME
    request = ""
    size = 2

    logging.info(f"Connected by : {addr}")
    response = "Welcome to budgetchat! What shall I call you?\n"
    send_and_log(conn, response, "new_client")

    while True:
        message = conn.recv(size)
        request += message.decode()
        if not message:
            # Client disconnected
            process_exiting_client(client_name, conn, addr)
            break
        if request.endswith("\n"):
            request = request.strip()
            logging.debug(f"Request : {request}")
            if client_name == DEFAULT_CLIENT_NAME:
                name = request
                if valid_name(name):
                    process_new_client(name, conn)
                    client_name = name
                else:
                    process_exiting_client(client_name, conn, addr)
                    break
            else:
                broadcast_to_all_except(request, client_name, chat_mode=True)
            request = ""


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}:{PORT}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(60)  # Set timeout for connection to 60 seconds.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
