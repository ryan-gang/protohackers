import json
from typing import Optional


def handle_request(request: bytes) -> bytes:
    reqs = request.decode().split("\n")
    responses = []

    for r in reqs:
        try:
            r = json.loads(r)
            if valid(r):
                prime = is_prime(r["number"])
                response = generate_response(prime)
            else:
                response = generate_response(None)
            responses.append(response)
        except json.JSONDecodeError:
            pass

    return ("\n".join(responses) + "\n").encode()


def generate_response(prime: Optional[bool]) -> str:
    # Conforming response : {"method":"isPrime","prime":false}
    # For non-conforming response return prime : None
    resp = {}
    resp["method"] = "isPrime"
    resp["prime"] = prime
    return json.dumps(resp)


def valid(request: dict[str, str]) -> bool:
    field1 = "method" in request
    value1 = request.get("method") == "isPrime"
    field2 = "number" in request
    value2 = type(request.get("number")) in [int, float]

    return field1 and value1 and field2 and value2


def is_prime(num: int | float) -> bool:
    if type(num) is not int:
        return False
    if num <= 1:
        return False
    if num == 2:
        return True
    if num % 2 == 0:
        return False
    sqrt_n = int(num**0.5)
    for i in range(3, sqrt_n + 1, 2):
        if num % i == 0:
            return False
    return True
