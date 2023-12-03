import asyncio
import logging
import sys
from asyncio import StreamReader, StreamWriter
from collections import defaultdict

from async_protocol import Parser, Reader, Serializer, Writer
from errors import ProtocolError
from messages import (CreatePolicy, DeletePolicy, DialAuthority, Error, Hello,
                      PolicyResult, SiteVisit, TargetPopulations)

logging.basicConfig(
    format=(
        "%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] | %(threadName)-10s |"
        " %(message)s"
    ),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="DEBUG",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "0.0.0.0", 9090
UPSTREAM_IP, UPSTREAM_PORT = "pestcontrol.protohackers.com", 20547
AUTHORITIES: dict[int, "AuthorityServer"] = {}  # {site_id: AuthorityServer}
# Add to list only after connecting and sending Hello.
POLICIES: dict[int, dict[str, tuple[int, int]]] = defaultdict(lambda: defaultdict(tuple[int, int]))
# {site_id : {species : [policy_id, action]}}
# 160 - Conserve, 144 - Cull
TARGETPOPULATIONS: dict[int, dict[str, tuple[int, int]]] = defaultdict(
    lambda: defaultdict(tuple[int, int])
)  # {site_id : {species : (min, max)}}


class AuthorityServer(object):
    def __init__(self, site_id: int) -> None:
        self.site_id = site_id

    async def connect(self):
        up_stream_reader, up_stream_writer = await asyncio.open_connection(
            UPSTREAM_IP, UPSTREAM_PORT
        )
        self.writer = Writer(up_stream_writer)
        self.parser = Parser()
        self.reader = Reader(up_stream_reader, self.parser)
        self.serializer = Serializer()
        self.connected = False
        upstream_peername = up_stream_writer.get_extra_info("peername")
        logging.debug(f"Connected to Authority @ {upstream_peername}")

    async def send_hello(self):
        out = Hello(protocol="pestcontrol", version=1)
        response = await self.serializer.serialize_hello(out)
        await self.writer.write(response)

    async def get_hello(self):
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code != 80 and not self.connected:
            raise ProtocolError("First message has to be Hello.")
        if msg_code == 80:
            _ = self.parser.parse_message(bytes(message_bytes))
            if not self.connected:
                self.connected = True

    async def handshake(self):
        await self.send_hello()
        await self.get_hello()

    async def dial_authority(self):
        out = DialAuthority(self.site_id)
        response = await self.serializer.serialize_dial_authority(out)
        await self.writer.write(response)

    async def get_target_populations(self):
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 84:
            population_target = self.parser.parse_message(bytes(message_bytes))
            assert type(population_target) == TargetPopulations
        else:
            raise ProtocolError("Unexpected Message")
        site = population_target.site
        populations = population_target.populations

        for population in populations:
            minimum, maximum = population.min, population.max
            (TARGETPOPULATIONS[site][population.species]) = minimum, maximum

    async def create_policy(
        self,
        site_id: int,
        species: str,
        action: bool,
    ):
        out = CreatePolicy(species, action)
        response = await self.serializer.serialize_create_policy(out)
        await self.writer.write(response)
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 87:
            policy_result = self.parser.parse_message(bytes(message_bytes))
            assert type(policy_result) == PolicyResult
        else:
            raise ProtocolError("Unexpected Message")
        policy_id = policy_result.policy
        POLICIES[site_id][species] = (policy_id, True)

    async def delete_policy(
        self,
        policy_id: int,
        site_id: int,
        species: str,
    ):
        out = DeletePolicy(policy_id)
        response = await self.serializer.serialize_delete_policy(out)
        await self.writer.write(response)
        POLICIES[site_id][species] = ()  # type: ignore
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 82:
            _ = self.parser.parse_message(bytes(message_bytes))
        else:
            raise ProtocolError("Unexpected Message")


async def client_handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    peername = stream_writer.get_extra_info("peername")
    logging.debug(f"Connected to client @ {peername}")

    writer = Writer(stream_writer)
    parser = Parser()
    reader = Reader(stream_reader, parser)
    serializer = Serializer()

    Connected = False

    out = Hello(protocol="pestcontrol", version=1)
    response = await serializer.serialize_hello(out)
    await writer.write(response)

    while 1:
        try:
            msg_code, message_bytes = await reader.read_message()
            if msg_code != 80 and not Connected:
                raise ProtocolError("First message has to be Hello.")
            if msg_code == 80:
                _ = parser.parse_message(bytes(message_bytes))
                if not Connected:
                    Connected = True
            elif msg_code == 88:  # SITEVISIT
                site_visit = parser.parse_message(bytes(message_bytes))
                assert type(site_visit) == SiteVisit
                site_id = site_visit.site
                if site_id not in AUTHORITIES:
                    authority = AuthorityServer(site_id)
                    await authority.connect()
                    await authority.handshake()
                    AUTHORITIES[site_id] = authority

                authority = AUTHORITIES[site_id]

                if site_id not in TARGETPOPULATIONS:
                    await authority.dial_authority()
                    await authority.get_target_populations()

                species_seen: set[str] = set()
                all_species_data: list[tuple[str, int]] = []
                for population in site_visit.populations:
                    species, count = population.species, population.count
                    species_seen.add(species)
                    all_species_data.append((species, count))

                targeted_species: set[str] = set(TARGETPOPULATIONS[site_id].keys())
                species_not_seen = targeted_species - species_seen

                for species in species_not_seen:
                    count = 0
                    all_species_data.append((species, count))

                for species, count in all_species_data:
                    if species in TARGETPOPULATIONS[site_id]:
                        minimum, maximum = TARGETPOPULATIONS[site_id][species]
                        if minimum <= count <= maximum:  # No Policy required
                            if POLICIES[site_id][species] != ():  # Policy present.
                                policy_id, _ = POLICIES[site_id][species]
                                await authority.delete_policy(policy_id, site_id, species)
                        else:
                            if count < minimum:  # Conserve
                                if POLICIES[site_id][species] != ():  # Policy present.
                                    policy_id, action = POLICIES[site_id][species]
                                    if action == 144:
                                        await authority.delete_policy(policy_id, site_id, species)
                                    elif action == 160:  # Correct policy present, just continue
                                        continue
                                await authority.create_policy(
                                    site_id, species, True
                                )  # Create Policy
                            else:  # CULL
                                if POLICIES[site_id][species] != ():  # Policy present.
                                    policy_id, action = POLICIES[site_id][species]
                                    if action == 144:
                                        continue
                                    elif action == 160:  # Wrong policy present : Delete
                                        await authority.delete_policy(policy_id, site_id, species)
                                await authority.create_policy(site_id, species, False)
            else:
                err = f"Illegal message type : {msg_code}"
                raise ProtocolError(err)
            await asyncio.sleep(0)
        except ProtocolError as err:
            logging.error(err)
            error = await serializer.serialize_error(Error(str(err)))
            await writer.write(error)
            await writer.close(peername)
            break
        except ConnectionResetError:
            logging.error("Client disconnected.")
            await writer.close(peername)
            break
        except asyncio.exceptions.IncompleteReadError:
            logging.error("Client disconnected.")
            break
    return


async def main():
    server = await asyncio.start_server(client_handler, IP, PORT)
    logging.info(f"Started Pest Control Server @ {IP}:{PORT}")

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.critical("Interrupted, shutting down.")
