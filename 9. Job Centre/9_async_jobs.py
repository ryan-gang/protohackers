import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from json import dumps

from async_helpers import Identifier, JobsHandler, Reader, Writer

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)

IP, PORT = "10.128.0.2", 9090

ID = Identifier()


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    client_uuid = str(uuid.uuid4()).split("-")[0]
    logging.info(
        f"Connected to client @ {stream_writer.get_extra_info('peername')}, referred to as"
        f" {client_uuid}"
    )
    reader = Reader(stream_reader)
    writer = Writer(stream_writer)
    jobs_handler = JobsHandler()
    client_working_on = 0

    while 1:
        response = {}
        try:
            request = await reader.readline()
            logging.debug(f"Req : {request}")
            data = jobs_handler.parse_request(request)

            type = data["request"]

            if type == "put":
                job_id = await ID.get_new()
                response = await jobs_handler.handle_put_request(data, job_id)

            elif type == "get":
                response, status = await jobs_handler.handle_get_request(data)
                if status:
                    client_working_on = response["id"]

            elif type == "delete":
                job_id = data["id"]
                response = await jobs_handler.handle_delete_request(job_id)
                if client_working_on == job_id:
                    client_working_on = 0

            elif type == "abort":
                job_id = data["id"]
                response = await jobs_handler.handle_abort_request(
                    job_id, client_working_on
                )

            else:
                response = {"status": "error", "error": "Unknown request type"}

            if response != {}:
                logging.debug(f"Res : {response}")
                await writer.writeline(dumps(response), client_uuid)
            await asyncio.sleep(0)

        except RuntimeError as err:
            logging.error(err)
            logging.debug(f"Res : {response}")
            response = {"status": "error", "error": str(err)}
            await writer.writeline(dumps(response), client_uuid)
        except (asyncio.exceptions.IncompleteReadError, ConnectionResetError):
            logging.error(f"Client {client_uuid} disconnected.")
            await writer.close(client_uuid)
            _ = await jobs_handler.handle_abort_request(client_working_on, client_working_on)
            break
    return


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started Jobs Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")
