import logging
import socket
import sys
import threading

from helpers import handle_request

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    stream=sys.stdout,
)

IP, PORT = "10.138.0.2", 9090


def handler(conn: socket.socket, addr: socket.AddressFamily):
    # Handler serves every connection, until it's closed.
    size = 1024
    with conn:
        logging.info(f"Connected by : {addr}")
        while True:
            request = bytearray()
            # If request size > size, we get incomplete requests
            # So, keep on receiving requests and append them
            # to a bytes array until the full request is complete
            # Request completion is confirmed by checking if the last char is \n.
            while ((slice := conn.recv(size)) and slice.decode()[-1] != "\n"):
                # As a result of the condition, the last valid slice won't be
                # processed here
                # logging.debug(slice)
                request.extend(slice)
            if not slice.strip():
                # Client disconnected
                logging.debug(f"Client disconnected : {addr}")
                break
            # Process the final slice where last char is \n.
            # As a result `request` is always filled with the entire request.
            # Even if a single line is passed, while loop wont run but this will.
            request.extend(slice)
            logging.debug(f"Request : {request}")
            response = handle_request(request)
            logging.debug(f"Response : {response}")
            conn.send(response)
            logging.info(f"Sent {len(response)} bytes to {addr}")


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
