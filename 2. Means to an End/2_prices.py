import logging
import socket
import struct
import sys
import threading
import warnings

from helpers import PriceAnalyzer

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    stream=sys.stdout,
)

IP, PORT = "10.138.0.2", 9090


def handler(conn: socket.socket, addr: socket.AddressFamily):
    # Handler serves every connection, until it's closed.
    size = 1024

    analyzer = PriceAnalyzer()
    # Unique and persistent analyzer for every unique client.

    with conn:
        logging.info(f"Connected by : {addr}")
        while True:
            request = bytearray()
            # If request size > size, we get incomplete requests
            # So, keep on receiving requests and append them
            # to a bytes array until the full request is complete
            # Request completion is confirmed by checking if len(request)%9 == 0.
            while True:
                slice = conn.recv(size)
                logging.debug(slice)
                request.extend(slice)
                if len(request) % 9 == 0:
                    break

            if not slice.strip() and not request:
                # Client disconnected
                logging.debug(f"Client disconnected : {addr}")
                break

            in_format, out_format = ">cii", ">i"

            for start in range(0, len(request), 9):
                part = request[start : start + 9]
                mode, arg_1, arg_2 = struct.unpack(in_format, part)

                # Might not be utf-8 decoding will fail, hence compare bytes.
                if mode == b"I":
                    seconds, price = arg_1, arg_2
                    analyzer.append_row(seconds, price)
                elif mode == b"Q":
                    start_time, end_time = arg_1, arg_2
                    mean_price = analyzer.get_mean(start_time, end_time)
                    response = struct.pack(out_format, mean_price)
                    logging.debug(f"Request : {request}")
                    logging.info(f"Response : {response}")
                    conn.send(response)
                    logging.info(f"Sent {len(response)} bytes to {addr}")
                else:
                    logging.debug(f"Unknown Mode passed : {mode}")
                    conn.send(b"")
                    logging.info(f"Sent {0} bytes to {addr}")


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}:{PORT}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(10)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
