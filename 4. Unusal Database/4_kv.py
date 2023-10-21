import logging
import socket
import sys
from collections import defaultdict

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
DATASTORE = defaultdict(str)
DATASTORE["version"] = "Ryan's KVStorev1"


def handler(request: bytes, addr: socket.AddressFamily):
    data, response = request.decode(), None
    if "=" in data:  # Insert
        key, *_ = data.split("=")
        value = "=".join(_)
        if key != "version":  # Don't allow updates for the `version` key
            DATASTORE[key] = value
    else:  # Retrieve
        key = data
        value = DATASTORE[key]
        response = f"{key}={value}"
    return response


def main():
    # Create a UDP socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the socket to a specific address and port
    s.bind((IP, PORT))
    logging.info(f"Started Server @ {IP}:{PORT}")

    while True:
        # Receive data from the client
        data, addr = s.recvfrom(1024)
        logging.info(f"Request : {data}")
        response = handler(data, addr)
        logging.info(f"Response : {response}")
        if response:
            s.sendto(response.encode(), addr)


if __name__ == "__main__":
    main()
