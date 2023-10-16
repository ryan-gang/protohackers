import re


def valid_name(name: str) -> bool:
    match = re.fullmatch("[A-Za-z0-9]{1,}", name)
    return match is not None
