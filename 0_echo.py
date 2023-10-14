import logging
import socket
import sys
import threading

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
            # request = conn.recv(size, socket.MSG_WAITALL) # MSG_WAITALL is blocking
            # Will hang on large messages. By design.
            # https://stackoverflow.com/questions/8470403/
            data = bytearray()
            while True:
                slice = conn.recv(size)
                logging.debug(slice)
                data.extend(slice)
                if not slice.strip():
                    logging.debug("Client disconnected.")
                    break
            logging.debug(data)
            logging.info(f"Sent {len(data)} bytes to {addr}")
            conn.send(data)
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
