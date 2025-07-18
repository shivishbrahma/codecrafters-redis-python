from typing import Tuple
import re
import struct
from .pub_redis import CLRF, ResponseDataType, Cache


def handle_request(request_buffer: bytes, cache: Cache) -> Tuple[bytes, bool]:
    request_str = request_buffer.decode("utf-8").strip()
    cmd = []
    if request_str.startswith(ResponseDataType.ARRAY.value):
        len_cmd = int(request_str.split(CLRF)[0][1:])
        for i in range(len_cmd):
            cmd.append(request_str.split(CLRF)[2 * (i + 1)])

    print(cmd)
    if cmd[0] == "ECHO":
        return (build_response(ResponseDataType.BULK_STRING, cmd[1]), False)

    if cmd[0] == "INFO":
        return (build_response(ResponseDataType.BULK_STRING, "role:master"), False)

    if cmd[0] == "SET":
        expire = -1
        unit = "s"
        if len(cmd) > 3:
            if cmd[3].lower() == "px":
                expire = int(cmd[4])
                unit = "ms"
            else:
                expire = int(cmd[4])
        cache.set(cmd[1], cmd[2], expire, unit)
        return (build_response(ResponseDataType.SIMPLE_STRING, "OK"), False)

    if cmd[0] == "GET":
        value = cache.get(cmd[1])
        if value:
            return (build_response(ResponseDataType.BULK_STRING, value.value), False)
        else:
            return (build_response(ResponseDataType.BULK_STRING, None), False)

    if cmd[0] == "DEL":
        cache.delete(cmd[1])
        return (build_response(ResponseDataType.SIMPLE_STRING, "OK"), False)

    if cmd[0] == "KEYS":
        keys = cache.keys(cmd[1])
        return (build_response(ResponseDataType.ARRAY, keys), False)

    if cmd[0] == "PING":
        return (build_response(ResponseDataType.SIMPLE_STRING, "PONG"), False)

    if cmd[0] == "CONFIG":
        if cmd[1] == "GET":
            return (
                build_response(
                    ResponseDataType.ARRAY, [cmd[2], cache.get_config(cmd[2])]
                ),
                False,
            )

    return (build_response(ResponseDataType.SIMPLE_ERROR, "Unknown command"), False)


def build_response(data_type: ResponseDataType, data=None) -> bytes:
    resp_buff = bytearray()
    if data_type == ResponseDataType.SIMPLE_STRING:
        resp_buff.extend(f"{data_type.value}{data}{CLRF}".encode())
    if data_type == ResponseDataType.BULK_STRING:
        data_len = len(data) if data else -1
        data = f"{CLRF}{data}" if data else ""
        resp_buff.extend(f"{data_type.value}{data_len}{data}{CLRF}".encode())
    if data_type == ResponseDataType.ARRAY:
        resp_buff.extend(f"{data_type.value}{len(data)}{CLRF}".encode())
        for item in data:
            resp_buff.extend(build_response(ResponseDataType.BULK_STRING, item))
    return resp_buff
