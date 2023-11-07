import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
from heapq import heappop, heappush
from json import dumps

from async_helpers import Identifier, Job, Reader, Writer, parse_request

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
DATASTORE: dict[int, Job] = {}
QUEUES: dict[str, list[tuple[int, int]]] = defaultdict(list)


async def handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    client_uuid = str(uuid.uuid4()).split("-")[0]
    logging.info(
        f"Connected to client @ {stream_writer.get_extra_info('peername')}, referred to as"
        f" {client_uuid}"
    )
    reader = Reader(stream_reader)
    writer = Writer(stream_writer)
    # client_working_on = 0

    try:
        while 1:
            request = await reader.readline()
            logging.debug(f"Req : {request}")
            data = parse_request(request)

            type = data["request"]

            if type == "put":
                queue, job, priority = data["queue"], data["job"], data["pri"]
                queue_str, job_str = dumps(queue), dumps(job)
                job_id = await ID.get_new()
                job_object = Job(job_id, job_str, priority, queue_str, status=0)
                DATASTORE[job_id] = job_object
                curr_queue = QUEUES[queue_str]
                heappush(curr_queue, (-priority, job_id))

                response = {"status": "ok", "id": job_id}
                await writer.writeline(dumps(response), client_uuid)

            if type == "get":
                queues = data["queues"]
                # wait_acceptable = data["wait"]
                temp: list[tuple[int, int, str]] = []
                for queue in queues:
                    queue_str = dumps(queue)
                    curr_queue = QUEUES[queue_str]
                    priority, job_id = heappop(curr_queue)
                    tup = (priority, job_id, queue_str)
                    heappush(temp, tup)

                final_job = heappop(temp)
                _, job_id, _ = final_job
                job_object = DATASTORE[job_id]

                for priority, job_id, queue_str in temp:
                    curr_queue = QUEUES[queue_str]
                    heappush(curr_queue, (priority, job_id))

                response = {
                    "status": "ok",
                    "id": job_id,
                    "job": job_object.job_data,
                    "pri": job_object.priority,
                    "queue": job_object.queue,
                }
                await writer.writeline(dumps(response), client_uuid)

            if type == "delete":
                job_id = data["id"]
                if job_id in DATASTORE:
                    DATASTORE.pop(job_id)
                    # Also handle removing from job queue, directly or indirectly
                    response = {"status": "ok"}
                else:
                    response = {"status": "no-job"}
                await writer.writeline(dumps(response), client_uuid)

            # logging.info(f"Res : {output}")
            # await writer.writeline(output, client_uuid)
            await asyncio.sleep(0)
            # Wait before the next iteration, or code gets stuck here, none of
            # the other clients are served. sleep(0) waits for the optimal
            # wait-time.
    except RuntimeError as E:
        logging.error(E)
        response = {"status": "error", "error": E}
        await writer.writeline(dumps(response), client_uuid)

    return


async def main():
    server = await asyncio.start_server(handler, IP, PORT)
    logging.info(f"Started Camera Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")
