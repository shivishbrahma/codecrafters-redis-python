from enum import Enum

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

    def set(self, key, value, expire=None):
        self.cache[key] = (value, expire)

    def get(self, key):
        return self.cache.get(key)