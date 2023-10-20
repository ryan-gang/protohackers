import logging
import re
import socket

DEFAULT_CLIENT_NAME = ""
CLIENTS: dict[str, socket.socket] = {}  # Name -> socket connection mapping for all clients.
# clients_lock = threading.RLock()  # Not really needed.


def valid_name(name: str) -> bool:
    match = re.fullmatch("[A-Za-z0-9]{1,}", name)
    return match is not None


def broadcast_room_details(client: str) -> None:
    # with clients_lock:
    conn = CLIENTS[client]
    participants = ", ".join(list(filter(lambda x: x != client, list(CLIENTS.keys()))))
    message = f"* The room contains: {participants}\n"
    send_and_log(conn, message, client)


def broadcast_to_all_except(text: str, excluded_client: str, chat_mode=False) -> None:
    # with clients_lock:
    logging.debug(f"Sending broadcast to {list(CLIENTS.keys())} except {excluded_client}")
    try:
        for name in CLIENTS:
            if name != excluded_client:
                conn = CLIENTS[name]
                if chat_mode:
                    response = f"[{excluded_client}] {text}\n"
                else:
                    response = text
                send_and_log(conn, response, name)
    except RuntimeError as E:
        logging.error(E)


def process_new_client(name: str, conn: socket.socket):
    # with clients_lock:
    CLIENTS[name] = conn
    joining_message = f"* {name} has entered the room\n"
    broadcast_to_all_except(joining_message, name)
    # if len(CLIENTS.keys()) > 1:
    broadcast_room_details(name)


def process_exiting_client(name: str, conn: socket.socket, addr, broadcast: bool = True):
    logging.debug(f"Client : {name} @ {addr} disconnected.")
    # with clients_lock:
    conn.close()
    if name != DEFAULT_CLIENT_NAME:
        if name in CLIENTS:
            CLIENTS.pop(name)
        exiting_message = f"* {name} has left the room\n"
        broadcast_to_all_except(exiting_message, name)


def send_and_log(conn: socket.socket, response: str, client_name: str):
    try:
        conn.send(response.encode())
        logging.info(f"Response : {response.strip()}")
        logging.info(f"Sent {len(response)} bytes to {client_name}")
    except Exception as E:
        logging.error(E)
