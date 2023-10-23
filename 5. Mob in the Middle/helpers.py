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


def get_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    stdout_handler = logging.StreamHandler(sys.stdout)
    file_handler = logging.FileHandler("app.log")
    fmt = (
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    )
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)
    file_handler.setFormatter(formatter)
    stdout_handler.setFormatter(formatter)

    logger.addHandler(stdout_handler)
    logger.addHandler(file_handler)

    return logger


UPSTREAM_IP, UPSTREAM_PORT = "chat.protohackers.com", 16963


def recv_data(conn: socket.socket):
    request, size = "", 32
    while True:
        message = conn.recv(size)
        request += message.decode()
        if not message:
            logging.info("Client disconnected.")
            return request
        if request.endswith("\n"):
            logging.debug(f"Request : {request}")
            return request


def send_data(conn: socket.socket, response: str):
    try:
        logging.debug(f"Response : {response}")
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
    pattern1 = r"^(7[A-Za-z0-9]{25,34}) "
    pattern2 = r" (7[A-Za-z0-9]{25,34})$"
    pattern3 = r"(7[A-Za-z0-9]{25,34}) "
    pattern4 = r"^(7[A-Za-z0-9]{25,34})$"

    all_matches: set[str] = set()
    for pattern in [pattern1, pattern2, pattern3, pattern4]:
        matches = re.findall(pattern, data)
        for match in matches:
            all_matches.add(match)

    for match in all_matches:
        data = data.replace(match, ADDRESS)
    return data
