import logging
import socket
import sys
import threading
from helpers import recv_data, send_data


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
UPSTREAM_IP, UPSTREAM_PORT = "chat.protohackers.com", 16963


def handler(c_conn: socket.socket, c_addr: socket.AddressFamily):
    """
    c_conn and c_addr correspond to conn and addr for client.
    UPSTREAM_SERVER   <-- -->   MITM_PROXY   <-- -->   CLIENT

    1. Open connection with upstream.

    2. Wait for recv() from upstream.
    3. Send data to client.
    4. Wait for recv() from client.
    5. Send data to upstream.
    6. GoTo 2.
    """
    logging.info(f"Connected by client : {c_addr}")
    # Open connection with upstream.
    upstream = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # IPv4, TCP
    upstream.connect((UPSTREAM_IP, UPSTREAM_PORT))
    logging.info(f"Connected to upstream : {UPSTREAM_IP}@{UPSTREAM_PORT}")

    flag = True
    while 1:
        if flag:
            data = recv_data(upstream)
            send_data(c_conn, data)
        else:
            data = recv_data(c_conn)
            send_data(upstream, data)
        flag ^= True


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}:{PORT}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(60)  # Set timeout for connection to 60 seconds.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
