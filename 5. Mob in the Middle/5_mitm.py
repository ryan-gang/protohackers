import logging
import socket
import sys
import threading
import time

from helpers import open_upstream_conn, recv_data, rewrite_address, send_data

IP, PORT = "10.138.0.2", 9090
logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


def handler(
    up_conn: socket.socket,
    up_addr: socket.AddressFamily,
    down_conn: socket.socket,
    down_addr: socket.AddressFamily,
):
    # One way connection from upstream to downstream.
    # Receive data from upstream, rewrite addresses and send it downstream.
    logging.info(f"Connected to downstream @ {down_addr} and upstream @ {up_addr}")

    while 1:
        try:
            data = recv_data(up_conn)
        except (ConnectionResetError, OSError) as E:
            logging.error(E)
            up_conn.close()
            down_conn.close()
            return
        new_data = rewrite_address(data)
        send_data(down_conn, new_data)
        time.sleep(1)  # Give other threads a chance to recv() data.


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started MITM Server @ {IP}:{PORT}")
    while True:
        # For every downstream client connection, open a corresponding upstream
        # connection to chat server.
        conn, addr = server_socket.accept()
        upstream, upstream_addr = open_upstream_conn()
        threading.Thread(target=handler, args=(upstream, upstream_addr, conn, addr)).start()
        threading.Thread(target=handler, args=(conn, addr, upstream, upstream_addr)).start()


if __name__ == "__main__":
    main()
