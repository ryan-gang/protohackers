import logging
import socket
import sys
import threading
from helpers import recv_data, send_data, open_upstream_conn, rewrite_address


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


def handler(
    up_conn: socket.socket,
    up_addr: socket.AddressFamily,
    down_conn: socket.socket,
    down_addr: socket.AddressFamily,
):
    logging.info(f"Connected to downstream @ {down_addr} and upstream @ {up_addr}")

    while 1:
        data = recv_data(up_conn)
        if not data:
            up_conn.close()
            down_conn.close()
            return
        data = rewrite_address(data)
        send_data(down_conn, data)


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started MITM Server @ {IP}:{PORT}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(60)  # Set timeout for connection to 60 seconds.
        upstream, upstream_addr = open_upstream_conn()
        threading.Thread(target=handler, args=(upstream, upstream_addr, conn, addr)).start()
        threading.Thread(target=handler, args=(conn, addr, upstream, upstream_addr)).start()


if __name__ == "__main__":
    main()
