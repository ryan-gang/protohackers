import logging
import re
import socket

UPSTREAM_IP, UPSTREAM_PORT = "chat.protohackers.com", 16963


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


def open_upstream_conn() -> tuple[socket.socket, str]:
    # Open connection with upstream.
    upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # IPv4, TCP
    upstream.connect((UPSTREAM_IP, UPSTREAM_PORT))
    upstream_addr = f"{UPSTREAM_IP}:{UPSTREAM_PORT}"
    logging.info(f"Connected to upstream : {upstream_addr}")
    return upstream, upstream_addr


def rewrite_address(data: str) -> str:
    ADDRESS = "7YWHMfk9JZe0LM0g1ZauHuiSxhI"

    pattern = "7[A-Za-z0-9]{25,34}"
    matches = re.findall(pattern, data)
    for match in matches:
        data = data.replace(match, ADDRESS)

    return data
