import asyncio
import logging
import struct
import sys
import uuid
from asyncio import StreamReader, StreamWriter
from collections import defaultdict

from async_protocol import Parser, Reader, Serializer, Writer
from errors import ProtocolError
from messages import (CreatePolicy, DeletePolicy, DialAuthority, Error, Hello,
                      PolicyResult, SiteVisit, TargetPopulations)

logging.basicConfig(
    format=("%(asctime)s | %(levelname)s | %(name)s |  [%(filename)s:%(lineno)d] |" " %(message)s"),
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler(sys.stdout)],
)
IP, PORT = "0.0.0.0", 9090
UPSTREAM_IP, UPSTREAM_PORT = "pestcontrol.protohackers.com", 20547
AUTHORITIES: dict[int, "AuthorityServer"] = {}  # {site_id: AuthorityServer}
# Add to list only after connecting & performing handshake.
# For every site_id we will instantiate a single AuthorityServer.
POLICIES: dict[int, dict[str, tuple[int, str]]] = defaultdict(lambda: defaultdict(tuple[int, str]))
# {site_id : {species : [policy_id, action]}}
# "CONSERVE", "CULL"
TARGETPOPULATIONS: dict[int, dict[str, tuple[int, int]]] = defaultdict(
    lambda: defaultdict(tuple[int, int])
)  # {site_id : {species : (min, max)}}
LOCK = asyncio.Lock()


class AuthorityServer(object):
    def __init__(self, site_id: int) -> None:
        self.site_id = site_id
        self.dialed_in = False
        self.uuid = "AU" + str(uuid.uuid4()).split("-")[0]

    async def connect(self):
        up_stream_reader, up_stream_writer = await asyncio.open_connection(
            UPSTREAM_IP, UPSTREAM_PORT
        )
        self.writer = Writer(up_stream_writer)
        self.parser = Parser()
        self.reader = Reader(up_stream_reader, self.parser)
        self.serializer = Serializer()
        self.connected = False
        self.upstream_peername = up_stream_writer.get_extra_info("peername")
        logging.debug(f"{self.uuid} | Connected to Authority @ {self.upstream_peername}")

    async def send_hello(self):
        out = Hello(protocol="pestcontrol", version=1)
        response = self.serializer.serialize_hello(out)
        await self.writer.write(response)
        logging.debug(f"{self.uuid} | Sent Hello to {self.upstream_peername}")

    async def get_hello(self):
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code != 80 and not self.connected:
            logging.error(f"{self.uuid} | {self.parser.parse_message(bytes(message_bytes))}")
            raise ProtocolError(f"First message has to be Hello. But received : {message_bytes}")
        if msg_code == 80:
            _ = self.parser.parse_message(bytes(message_bytes))
            if not self.connected:
                self.connected = True
        logging.debug(f"{self.uuid} | Received Hello from {self.upstream_peername}")

    async def handshake(self):
        await self.send_hello()
        await self.get_hello()
        logging.debug(f"{self.uuid} | Finished handshake with {self.upstream_peername}")

    async def dial_authority(self):
        out = DialAuthority(self.site_id)
        response = self.serializer.serialize_dial_authority(out)
        await self.writer.write(response)
        logging.debug(
            f"{self.uuid} | Sent Dial Authority for {self.site_id} to {self.upstream_peername}"
        )
        self.dialed_in = True

    async def fetch_target_populations(self) -> TargetPopulations:
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 84:
            population_target = self.parser.parse_message(bytes(message_bytes))
            assert type(population_target) == TargetPopulations
        else:
            logging.error(f"{self.uuid} | {self.parser.parse_message(bytes(message_bytes))}")
            raise ProtocolError("Unexpected Message. Expected 84")
        logging.debug(
            f"{self.uuid} | Received Target Population for {self.site_id} from {self.upstream_peername}"
        )
        # logging.debug(f"{self.uuid} | {population_target}")
        return population_target

    async def parse_target_populations(self, population_target: TargetPopulations):
        site = population_target.site
        populations = population_target.populations

        for population in populations:
            minimum, maximum = population.min, population.max
            (TARGETPOPULATIONS[site][population.species]) = minimum, maximum
        logging.debug(f"Parsed {len(populations)} and added them to TARGETPOPULATIONS[{site}]")

    async def get_target_populations(self):
        if not self.dialed_in:
            await self.dial_authority()
            populations = await self.fetch_target_populations()
            self.populations = populations  # Cached
        if self.site_id not in TARGETPOPULATIONS:
            await self.parse_target_populations(self.populations)

    async def create_policy(
        self,
        site_id: int,
        species: str,
        action: str,
    ):
        out = CreatePolicy(species, action)
        response = self.serializer.serialize_create_policy(out)
        await self.writer.write(response)
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 87:
            policy_result = self.parser.parse_message(bytes(message_bytes))
            assert type(policy_result) == PolicyResult
        else:
            logging.error(f"{self.uuid} | {self.parser.parse_message(bytes(message_bytes))}")
            raise ProtocolError("Unexpected Message. Expected 87")
        policy_id = policy_result.policy
        logging.debug(
            f"{self.uuid} | Created policy for {site_id}, {species}, {action} : {policy_id}"
        )
        POLICIES[site_id][species] = (policy_id, action)

    async def delete_policy(
        self,
        policy_id: int,
        site_id: int,
        species: str,
    ):
        out = DeletePolicy(policy_id)
        response = self.serializer.serialize_delete_policy(out)
        await self.writer.write(response)
        logging.debug(f"{self.uuid} | Deleted policy ID {policy_id} for {site_id}, {species}")
        POLICIES[site_id][species] = ()  # type: ignore
        msg_code, message_bytes = await self.reader.read_message()
        if msg_code == 82:
            _ = self.parser.parse_message(bytes(message_bytes))
        else:
            logging.error(f"{self.uuid} | {self.parser.parse_message(bytes(message_bytes))}")
            raise ProtocolError("Unexpected Message. Expected 82")
        logging.debug(f"{self.uuid} | Received OK for Delete Policy : {policy_id}")


async def client_handler(stream_reader: StreamReader, stream_writer: StreamWriter):
    peername = stream_writer.get_extra_info("peername")
    client_uuid = "CL" + str(uuid.uuid4()).split("-")[0]
    logging.debug(f"{client_uuid} | Connected to client @ {peername}")

    writer = Writer(stream_writer)
    parser = Parser()
    reader = Reader(stream_reader, parser)
    serializer = Serializer()

    connected = False

    out = Hello(protocol="pestcontrol", version=1)
    response = serializer.serialize_hello(out)
    await writer.write(response)

    while 1:
        try:
            msg_code, message_bytes = await reader.read_message()
            if msg_code != 80 and not connected:
                raise ProtocolError("First message has to be Hello.")
            if msg_code == 80:
                _ = parser.parse_message(bytes(message_bytes))
                if not connected:
                    connected = True
                elif connected:
                    raise ProtocolError("Illegal Hello Message")
            elif msg_code == 88:  # SITEVISIT
                site_visit = parser.parse_message(bytes(message_bytes))
                # logging.debug(f"{client_uuid} | {site_visit}")

                assert type(site_visit) == SiteVisit
                site_id = site_visit.site

                if site_id not in AUTHORITIES:
                    authority = AuthorityServer(site_id)
                    await authority.connect()
                    await authority.handshake()
                    AUTHORITIES[site_id] = authority

                authority = AUTHORITIES[site_id]
                logging.debug(f"Linked : {client_uuid} x {authority.uuid}")
                await authority.get_target_populations()

                all_species_data: dict[str, int] = defaultdict(int)
                for population in site_visit.populations:
                    species, count = population.species, population.count
                    if species in all_species_data and all_species_data[species] != count:
                        raise ProtocolError(
                            f"Multiple conflicting counts for the same species : {species}"
                        )
                    all_species_data[species] = count

                for species in TARGETPOPULATIONS[site_id]:
                    if species not in all_species_data:
                        all_species_data[species] = 0

                for species in all_species_data:
                    count = all_species_data[species]
                    async with LOCK:
                        if species in TARGETPOPULATIONS[site_id]:
                            minimum, maximum = TARGETPOPULATIONS[site_id][species]
                            logging.debug(
                                f"{client_uuid} | Site : {site_id} contains {species} : {count}, expected : [{minimum}, {maximum}]"
                            )
                            logging.debug(
                                f"{client_uuid} | Policy @ {site_id} for {species} : {POLICIES[site_id][species]}"
                            )
                            if count < minimum:  # Conserve
                                if POLICIES[site_id][species] != ():  # Policy present.
                                    policy_id, action = POLICIES[site_id][species]
                                    if action == "CULL":
                                        await authority.delete_policy(policy_id, site_id, species)
                                        await authority.create_policy(
                                            site_id, species, "CONSERVE"
                                        )  # Create Policy
                                else:
                                    await authority.create_policy(
                                        site_id, species, "CONSERVE"
                                    )  # Create Policy
                            elif count > maximum:  # CULL
                                if POLICIES[site_id][species] != ():  # Policy present.
                                    policy_id, action = POLICIES[site_id][species]
                                    if action == "CONSERVE":  # Wrong policy present : Delete
                                        await authority.delete_policy(policy_id, site_id, species)
                                        await authority.create_policy(site_id, species, "CULL")
                                else:
                                    await authority.create_policy(site_id, species, "CULL")
                            else:  # minimum <= count <= maximum:  No Policy required
                                if POLICIES[site_id][species] != ():  # Policy present.
                                    policy_id, _ = POLICIES[site_id][species]
                                    await authority.delete_policy(policy_id, site_id, species)

            else:
                err = f"Illegal message type : {msg_code}"
                raise ProtocolError(err)
            await asyncio.sleep(0)
        except (struct.error, ProtocolError) as err:
            logging.error(f"{client_uuid} | {err}")
            error = serializer.serialize_error(Error(str(err)))
            await writer.write(error)
            await writer.close(peername)
            break
        except ConnectionResetError:
            logging.error(f"{client_uuid} | Client disconnected.")
            await writer.close(peername)
            break
        except asyncio.exceptions.IncompleteReadError:
            logging.error(f"{client_uuid} | Client disconnected.")
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
