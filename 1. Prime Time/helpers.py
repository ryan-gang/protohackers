import json
from typing import Optional


def valid(request: dict[str, str]) -> bool:
    field1 = "method" in request
    value1 = request["method"] == "isPrime"
    field2 = "number" in request
    value2 = type(request["number"]) in [int, float]

    return field1 and value1 and field2 and value2


def generate_response(prime: Optional[bool]) -> bytes:
    # Conforming response : {"method":"isPrime","prime":false}
    # For non-conforming response return prime : None
    resp = {}
    resp["method"] = "isPrime"
    resp["prime"] = prime
    return (json.dumps(resp) + "\n").encode()


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
            print(i)
            return False
    return True
