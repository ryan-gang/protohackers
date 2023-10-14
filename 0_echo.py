import socket
import sys
import threading


def handle_request(conn: socket.socket, addr: socket.AddressFamily, args: list[str]):
    # This func runs in every new thread, parsing the request, creating a response
    # Sending it out, closing connection and closing thread.
    size = 1024
    with conn:
        print("Connected by :", addr)
        try:
            request = conn.recv(size)
            if not request:
                print("Client disconnected.")
                return
            print(request)
            conn.send(request)
        except Exception:
            return


def main():
    args = sys.argv
    IP, PORT = "10.138.0.2", 9090
    server_socket = socket.create_server((IP, PORT), reuse_port=True)
    print(f"Started Server @ {IP}")
    while True:
        conn, addr = server_socket.accept()
        conn.settimeout(30)  # Set timeout for connection to 30 seconds.
        # Spin out a new thread to process this request, return control back to main thread.
        threading.Thread(target=handle_request, args=(conn, addr, args)).start()


if __name__ == "__main__":
    main()
