import logging
import socket
import sys
import threading
import time

from heartbeat import heartbeat_deregister_client, heartbeat_register_client, heartbeat_thread
from protocol import Parser, Serializer, SocketHandler

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090
parser = Parser()
serializer = Serializer()
sock_handler = SocketHandler()


def handler(conn: socket.socket, addr: socket.AddressFamily):
    logging.info(f"Connected to client @ {addr}")
    while 1:
        try:
            msg_type, data = sock_handler.read_data(conn)
            logging.debug(f"Req : {msg_type} : {data} as hex : {data.hex()}")

            if msg_type == "20":
                logging.info(f"Message: {parser.parse_plate_data(data)}")
            elif msg_type == "40":
                interval, _ = parser.parse_wantheartbeat_data(data)
                if interval > 0:
                    heartbeat_register_client(conn, interval // 10)
                logging.info(f"Message: WantHeartBeat @ {interval//10} seconds.")
            elif msg_type == "80":
                logging.info(f"Message: {parser.parse_iamcamera_data(data)}")
            elif msg_type == "81":
                logging.info(f"Message: {parser.parse_iamdispatcher_data(data)}")
            else:
                err = serializer.serialize_error_data(msg="Unknown message type")
                sock_handler.send_data(conn, err.decode())
        except (ConnectionResetError, OSError) as E:
            logging.error(E)
            heartbeat_deregister_client(conn)
            conn.close()
            return

        time.sleep(1)  # Give other threads a chance.


def main():
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    logging.info(f"Started Server @ {IP}")
    threading.Thread(target=heartbeat_thread, daemon=True).start()
    while True:
        conn, addr = server_socket.accept()
        # conn.settimeout(10)  # Set timeout for connection to 10 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handler, args=(conn, addr)).start()


if __name__ == "__main__":
    main()
