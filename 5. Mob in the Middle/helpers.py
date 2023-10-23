import logging
import re
import socket
import sys

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

UPSTREAM_IP, UPSTREAM_PORT = "chat.protohackers.com", 16963


def recv_data(conn: socket.socket):
    request, size = "", 32
    while True:
        message = conn.recv(size)
        request += message.decode()
        if not message:
            raise ConnectionResetError("Client Disconnected.")
        if request.endswith("\n"):
            logging.debug(f"Request : {request.strip()}")
            return request


def send_data(conn: socket.socket, response: str):
    try:
        logging.debug(f"Response : {response.strip()}")
        conn.send(response.encode())
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
    pattern = r"(?=(?: |^)(7[A-Za-z0-9]{25,34})(?: |$))"

    matches = re.findall(pattern, data)
    for match in matches:
        if match:
            data = data.replace(match, ADDRESS)
    return data
