from typing import Tuple
import re
import struct
from .pub_redis import build_response, CLRF, ResponseDataType


def handle_request(request_buffer: bytes) -> Tuple[bytes, bool]:
    request_str = request_buffer.decode("utf-8").strip()
    cmd = []
    if request_str.startswith(ResponseDataType.ARRAY.value):
        len_cmd = int(request_str.split(CLRF)[0][1:])
        for i in range(len_cmd):
            cmd.append(request_str.split(CLRF)[2 * (i + 1)])

    print(cmd)
    if cmd[0] == "ECHO":
        return (build_response(ResponseDataType.BULK_STRING, cmd[1]), False)

    return (build_response(ResponseDataType.SIMPLE_STRING, "PONG"), False)
