import asyncio
import json
import logging
import sys
from asyncio import StreamReader, StreamWriter
from collections import defaultdict
from heapq import heappop, heappush
from json import dumps, loads
from typing import Any

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)


class Job(object):
    def __init__(
        self, id: int, job_data: str, priority: int, queue: str, status: int
    ) -> None:
        self.id = id
        self.job_data = job_data
        self.priority = priority
        self.queue = queue
        self.status = status

    def __repr__(self) -> str:
        return (
            f"Job details : \nid : {self.id}\njob : {self.job_data}\npriority :"
            f" {self.priority}\nqueue : {self.queue}\nstatus : {self.status}\n"
        )


class Reader(object):
    def __init__(self, reader: StreamReader) -> None:
        self.reader = reader

    async def readline(self) -> str:
        data = await self.reader.readuntil(separator=b"\n")
        if not data:
            raise RuntimeError("Connection closed by client")
        decoded = data.decode("utf-8").strip()
        return decoded

    async def read(self) -> str:
        line = bytearray()
        while True:
            byte = await self.reader.readexactly(1)
            if byte == b"":
                raise RuntimeError("Connection closed by client")
            line.extend(byte)
            if byte == b"\n":
                break
        decoded = line.decode("utf-8").strip()
        return decoded


class Writer(object):
    def __init__(self, writer: StreamWriter) -> None:
        self.writer = writer
        self.byte_counter = 0

    async def writeline(self, data: str, client: str):
        data = data + "\n"
        out = data.encode("utf-8")
        self.writer.write(out)
        logging.debug(f"Sent {out.hex()} : {len(data)} bytes to {client}")
        await self.writer.drain()
        return

    async def close(self, client_uuid: str):
        self.writer.write_eof()
        self.writer.close()
        logging.debug(f"Closed connection to client @ {client_uuid}.")
        return


class Identifier(object):
    def __init__(self) -> None:
        self.id = 0

    async def get_new(self) -> int:
        self.id += 1
        return self.id


DATASTORE: dict[int, Job] = {}
QUEUES: dict[str, list[tuple[int, int]]] = defaultdict(list)
DELETED_JOBS: set[int] = set()


class JobsHandler(object):
    def __init__(self):
        pass

    def parse_request(self, data: str) -> dict[str, Any]:
        """
        Convert json request to python dict, and check for its validity.
        """
        json_decoding_success = False
        try:
            req = json.loads(data)
            json_decoding_success = True
        except json.JSONDecodeError:
            raise RuntimeError("JSON Decode Error")

        # Check if request is valid
        request_types = ["put", "get", "delete", "abort"]
        c1 = "request" in req
        try:
            c2 = req["request"] in request_types
        except KeyError:
            c2 = False
        c3 = json_decoding_success
        valid = c1 and c2 and c3

        if not valid:
            raise RuntimeError("Invalid request received")
        return req

    async def handle_put_request(
        self, data: dict[str, Any], job_id: int
    ) -> dict[str, Any]:
        queue, job, priority = data["queue"], data["job"], data["pri"]
        queue_str, job_str = dumps(queue), dumps(job)
        job_object = Job(job_id, job_str, priority, queue_str, status=0)
        DATASTORE[job_id] = job_object
        curr_queue = QUEUES[queue_str]
        heappush(curr_queue, (-priority, job_id))

        logging.debug(f"PUT : {job_id}")
        return {"status": "ok", "id": job_id}

    async def handle_get_request(
        self, data: dict[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        queues = data["queues"]
        if "wait" in data:
            wait_acceptable = bool(data["wait"])
        else:
            wait_acceptable = False
        available_jobs: list[tuple[int, int, str]] = []

        while 1:
            for queue in queues:
                queue_str = dumps(queue)
                curr_queue = QUEUES[queue_str]
                while len(curr_queue) > 0:
                    priority, job_id = heappop(curr_queue)
                    if job_id in DELETED_JOBS:
                        continue
                    tup = (priority, job_id, queue_str)
                    heappush(available_jobs, tup)
                    break

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

                for priority, job_id, queue_str in available_jobs:
                    curr_queue = QUEUES[queue_str]
                    heappush(curr_queue, (priority, job_id))
                return response, True
            else:
                if not wait_acceptable:
                    response = {"status": "no-job"}
                    return response, False
                else:
                    await asyncio.sleep(2)

    async def handle_delete_request(self, job_id: int) -> dict[str, Any]:
        if job_id in DATASTORE:
            DATASTORE.pop(job_id)
            DELETED_JOBS.add(job_id)
            # Handle jobs already queued in "get" method
            logging.debug(f"DELETE : {job_id}")
            return {"status": "ok"}
        logging.debug(f"DELETE FAILED : {job_id}")
        return {"status": "no-job"}

    async def handle_abort_request(
        self, job_id: int, client_working_on: int
    ) -> dict[str, Any]:
        if job_id in DATASTORE:
            if job_id == client_working_on:
                job_object = DATASTORE[job_id]
                queue, priority = job_object.queue, job_object.priority
                curr_queue = QUEUES[queue]
                heappush(curr_queue, (-priority, job_id))

                logging.debug(f"ABORT : {job_id}")
                return {"status": "ok"}
            else:
                raise RuntimeError("Invalid abort request from client")
        else:
            logging.debug(f"ABORT FAILED : {job_id}")
            return {"status": "no-job"}
