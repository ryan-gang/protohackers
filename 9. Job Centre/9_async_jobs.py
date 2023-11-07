import asyncio
import logging
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
from heapq import heappop, heappush
from json import dumps, loads

from async_helpers import Identifier, Job, Reader, Writer, parse_request

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
    client_working_on = 0

    while 1:
        try:
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

            elif type == "get":
                queues = data["queues"]
                wait_acceptable = data["wait"]
                available_jobs: list[tuple[int, int, str]] = []

                while 1:
                    for queue in queues:
                        queue_str = dumps(queue)
                        curr_queue = QUEUES[queue_str]
                        if len(curr_queue) > 0:
                            priority, job_id = heappop(curr_queue)
                            tup = (priority, job_id, queue_str)
                            heappush(available_jobs, tup)

                    if len(available_jobs) > 0:
                        final_job = heappop(available_jobs)
                        _, job_id, _ = final_job
                        job_object = DATASTORE[job_id]
                        response = {
                            "status": "ok",
                            "id": job_id,
                            "job": loads(job_object.job_data),
                            "pri": job_object.priority,
                            "queue": loads(job_object.queue),
                        }
                        client_working_on = job_id

                        for priority, job_id, queue_str in available_jobs:
                            curr_queue = QUEUES[queue_str]
                            heappush(curr_queue, (priority, job_id))
                        break
                    else:
                        if not wait_acceptable:
                            response = {"status": "no-job"}
                            break
                        else:
                            await asyncio.sleep(2)

            elif type == "delete":
                job_id = data["id"]
                if client_working_on == job_id:
                    client_working_on = 0
                if job_id in DATASTORE:
                    DATASTORE.pop(job_id)
                    # Also handle removing from job queue, directly or indirectly
                    response = {"status": "ok"}
                else:
                    response = {"status": "no-job"}

            elif type == "abort":
                job_id = data["id"]
                if job_id in DATASTORE:
                    if job_id == client_working_on:
                        job_object = DATASTORE[job_id]
                        queue, priority = job_object.queue, job_object.priority
                        curr_queue = QUEUES[queue]
                        heappush(curr_queue, (-priority, job_id))
                        response = {"status": "ok"}
                    else:
                        raise RuntimeError("Invalid abort request from client")
                else:
                    response = {"status": "no-job"}

            else:
                response = {"status": "error", "error": "Unknown request type"}

            logging.debug(f"Res : {response}")
            await writer.writeline(dumps(response), client_uuid)
            await asyncio.sleep(0)

        except RuntimeError as err:
            logging.error(err)
            response = {"status": "error", "error": err}
            await writer.writeline(dumps(response), client_uuid)
        except asyncio.exceptions.IncompleteReadError as err:
            logging.error(f"Client disconnected : {err}")
            await writer.close(client_uuid)
            if (job_id := client_working_on) != 0:
                if job_id in DATASTORE:
                    job_object = DATASTORE[job_id]
                    queue, priority = job_object.queue, job_object.priority
                    curr_queue = QUEUES[queue]
                    heappush(curr_queue, (-priority, job_id))
            break
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
