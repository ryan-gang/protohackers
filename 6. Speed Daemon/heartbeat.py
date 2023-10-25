import socket
import time

from protocol import Serializer, SocketHandler


class Heartbeat(object):
    def __init__(self):
        self.clients_to_serve: dict[socket.socket, int] = {}
        self.serializer = Serializer()
        self.sock_handler = SocketHandler()
        self.msg = self.generate_heartbeat().decode()

    def send_heartbeat(self, client: socket.socket):
        self.sock_handler.send_data(client, self.msg)

    def generate_heartbeat(self) -> bytes:
        return self.serializer.serialize_heartbeat_data()

    def register_client(self, client: socket.socket, interval: int):
        # interval is in deciseconds
        self.clients_to_serve[client] = interval // 10

    def deregister_client(self, client: socket.socket):
        if client in self.clients_to_serve:
            self.clients_to_serve.pop(client)

    def heartbeats(self):
        elapsed = 0
        sleep_interval = 0.5  # seconds

        while 1:
            time.sleep(sleep_interval)
            elapsed += sleep_interval
            for client in self.clients_to_serve:
                interval = self.clients_to_serve[client]
                if elapsed == interval:
                    self.send_heartbeat(client)
