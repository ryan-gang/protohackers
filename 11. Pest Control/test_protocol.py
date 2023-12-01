from async_protocol import Parser
from messages import (OK, CreatePolicy, DeletePolicy, DialAuthority, Error,
                      Hello, PolicyResult, PopulationActual, SiteVisit,
                      TargetPopulations)


class TestHello:
    parser = Parser()
    hexdigest = "50000000190000000b70657374636f6e74726f6c00000001ce"
    data = bytes.fromhex(hexdigest)
    hello: Hello = parser.parse_message(data)

    def test_parse_hello_protocol(self):
        assert self.hello.protocol == "pestcontrol"

    def test_parse_hello_version(self):
        assert self.hello.version == 1


class TestError:
    parser = Parser()
    hexdigest = "510000000d0000000362616478"
    data = bytes.fromhex(hexdigest)
    error: Error = parser.parse_message(data)

    def test_message(self):
        assert self.error.message == "bad"


class TestOK:
    parser = Parser()
    hexdigest = "5200000006a8"
    data = bytes.fromhex(hexdigest)
    ok: OK = parser.parse_message(data)

    def test_ok(self):
        assert isinstance(self.ok, OK)


class TestDialAuthority:
    parser = Parser()
    hexdigest = "530000000a000030393a"
    data = bytes.fromhex(hexdigest)
    dial_authority: DialAuthority = parser.parse_message(data)

    def test_site(self):
        assert self.dial_authority.site == 12345


class TestPopulationTarget:
    parser = Parser()
    hexdigest = (
        "540000002c000030390000000200000003646f67000000010000000300000003726174000000000000000a80"
    )
    data = bytes.fromhex(hexdigest)
    population_target: TargetPopulations = parser.parse_message(data)

    def test_site(self):
        assert self.population_target.site == 12345

    def test_array_len(self):
        assert len(self.population_target.populations) == 2

    def test_species(self):
        assert self.population_target.populations[0].species == "dog"

    def test_min(self):
        assert self.population_target.populations[0].min == 1

    def test_max(self):
        assert self.population_target.populations[0].max == 3


class TestCreatePolicy:
    parser = Parser()
    hexdigest = "550000000e00000003646f67a0c0"
    data = bytes.fromhex(hexdigest)
    create_policy: CreatePolicy = parser.parse_message(data)

    def test_species(self):
        assert self.create_policy.species == "dog"

    def test_action(self):
        assert self.create_policy.action is True


class TestDeletePolicy:
    parser = Parser()
    hexdigest = "560000000a0000007b25"
    data = bytes.fromhex(hexdigest)
    delete_policy: DeletePolicy = parser.parse_message(data)

    def test_policy(self):
        assert self.delete_policy.policy == 123


class TestPolicyResult:
    parser = Parser()
    hexdigest = "570000000a0000007b24"
    data = bytes.fromhex(hexdigest)
    policy_result: PolicyResult = parser.parse_message(data)

    def test_policy(self):
        assert self.policy_result.policy == 123


class TestSiteVisit:
    parser = Parser()
    hexdigest = "5800000024000030390000000200000003646f670000000100000003726174000000058c"
    data = bytes.fromhex(hexdigest)
    site_visit: SiteVisit = parser.parse_message(data)

    def test_array_len(self):
        assert len(self.site_visit.populations) == 2

    def test_site(self):
        assert self.site_visit.site == 12345

    def test_type_1(self):
        assert isinstance(self.site_visit.populations[0], PopulationActual)

    def test_type_2(self):
        assert isinstance(self.site_visit.populations[1], PopulationActual)

    def test_species(self):
        assert self.site_visit.populations[0].species == "dog"

    def test_count(self):
        assert self.site_visit.populations[0].count == 1
