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


def build_response(
    data_type: ResponseDataType, data: str = None, error: str = None
) -> bytes:
    resp_buff = bytearray()
    if data_type == ResponseDataType.SIMPLE_STRING:
        resp_buff.extend(f"{data_type.value}{data}{CLRF}".encode())
    if data_type == ResponseDataType.BULK_STRING:
        resp_buff.extend(f"{data_type.value}{len(data)}{CLRF}{data}{CLRF}".encode())
    return resp_buff
