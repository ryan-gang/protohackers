import asyncio
import logging
import sys

from async_helpers import close, read, write, broadcast, CLIENTS, valid_name

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "10.154.0.3", 9090


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    conn = writer.get_extra_info("peername")
    logging.info(f"Connected to client @ {conn}")

    # Prompt client for name
    HELLO_MSG = "Welcome to budgetchat! What shall I call you?"
    await write(writer, HELLO_MSG)
    name = await read(reader)

    # Check if name is valid or not, process accordingly.
    if valid_name(name):
        PARTICIPANTS_MSG = f"* The room contains: {', '.join(CLIENTS.keys())}"
        CLIENTS[name] = (reader, writer)
        JOINED_MSG = f"* {name} has entered the room"
        await broadcast(JOINED_MSG, name)
        await write(writer, PARTICIPANTS_MSG)
    else:
        await close(writer, "Invalid name received.", conn)
        return

    # After client has joined, read and broadcast all messages, until client leaves.
    while 1:
        msg = await read(reader)
        if not msg:
            CLIENT_LEFT_MSG = f"* {name} has left the room"
            await broadcast(CLIENT_LEFT_MSG, name)
            CLIENTS.pop(name)
            await close(writer, f"Client : {name} disconnected", conn)
            return
        message = f"[{name}] {msg}"
        await broadcast(message, name)


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started Chat Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")
