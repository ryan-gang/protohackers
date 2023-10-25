import logging
import socket
import sys
import threading
import time
from helpers import recv_data

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.138.0.2", 9090


def handler(conn: socket.socket, addr: socket.AddressFamily):
    logging.info(f"Connected to client @ {addr}")
    while 1:
        try:
            data = recv_data(conn)
            logging.info(data)
        except (ConnectionResetError, OSError) as E:
            logging.error(E)
            conn.close()
            return

        time.sleep(1)  # Give other threads a chance to recv() data.


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(10)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
