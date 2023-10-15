import json
import logging
import socket
import sys
import threading
from helpers import valid, generate_response, is_prime

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    stream=sys.stdout,
)


def handle_request(conn: socket.socket, addr: socket.AddressFamily):
    # This func runs in every new thread, parsing the request, creating a response
    # Sending it out, closing connection and closing thread.
    size = 1024
    with conn:
        logging.info(f"Connected by : {addr}")
        try:
            while True:
                request = conn.recv(size)
                logging.debug(request)
                if not request.strip():
                    logging.debug("Client disconnected.")
                    break
                request = json.loads(request.decode())
                if valid(request):
                    prime = is_prime(request["number"])
                    response = generate_response(prime)
                else:
                    response = generate_response(None)

                logging.debug(response)
                conn.send(response)
                logging.info(f"Sent {len(response)} bytes to {addr}")
        except Exception:
            return


def main():
    IP, PORT = "10.138.0.2", 9090
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(10)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handle_request, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
