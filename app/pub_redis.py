from enum import Enum
import threading


CLRF = "\r\n"


class ResponseDataType(Enum):
    SIMPLE_STRING = "+"
    SIMPLE_ERROR = "-"
    INTEGER = ":"
    BULK_STRING = "$"
    ARRAY = "*"
    NULL = "_"
    BOOLEAN = "#"
    DOUBLE = ","
    BIG_NUMBER = "("
    BULK_ERROR = "!"
    VERBATIM_STRING = "="
    MAP = "%"
    ATTRIBUTE = "|"
    SETS = "~"
    PUSH = ">"


class Cache:
    def __init__(self):
        self.cache = {}

    def set(self, key: str, value: str, expire: float = -1):
        self.cache[key] = value

        if expire > 0:
            threading.Timer(expire, self.delete, args=[key]).start()

    def get(self, key: str):
        return self.cache.get(key)

    def delete(self, key: str):
        self.cache.pop(key, None)
