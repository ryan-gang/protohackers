from dataclasses import dataclass


@dataclass
class Hello:
    protocol: str
    version: int


@dataclass
class Error:
    message: str


@dataclass
class OK:
    pass


@dataclass
class DialAuthority:
    site: int


@dataclass
class PopulationTarget:
    species: str
    min: int
    max: int


@dataclass
class TargetPopulations:
    site: int
    populations: list[PopulationTarget]


@dataclass
class CreatePolicy:
    species: str
    action: str


@dataclass
class DeletePolicy:
    policy: int


@dataclass
class PolicyResult:
    policy: int


@dataclass
class PopulationActual:
    species: str
    count: int


@dataclass
class SiteVisit:
    site: int
    populations: list[PopulationActual]
