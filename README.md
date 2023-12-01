# Protohackers

## Description

This repository contains solutions to all the Protohackers server problems, which involve writing high-performance TCP and UDP servers from scratch. The solutions are written in Python `3.11`, with the first six leveraging `Multi-threading` and the rest using `asyncio`. The code is designed to be both maintainable and readable. All servers have been tested on Google Cloud Platform's `e2-micro` virtual machines.

## Getting Started

### Dependencies

- Python >= `3.11`
- Pandas >= `2.1.1`

### Executing

- Clone the repository : `git clone https://github.com/ryan-gang/protohackers.git`
- Create virtualenv : `python -m venv venv`
- Activate virtualenv : `source venv/bin/activate`
- Install dependencies : `pip install -r requirements.txt`
- Run : `python server.py`

For the servers to operate on Google Cloud Platform (GCP), it's necessary to establish an ingress firewall rule that opens port `9090` for both `TCP` and `UDP` protocols.