from typing import Tuple
import re
import socket
from .pub_redis import CLRF, RedisDataType, RedisCache, RedisEntity, RedisRDBFile

# def parse_request


def handle_request(request_buffer: bytes, cache: RedisCache) -> Tuple[bytes, bool]:
    request_entity = RedisEntity.from_buffer(request_buffer)
    print(request_entity.data[0])

    cmd = str(request_entity.data[0])
    if len(request_entity.data) > 1:
        params = [str(param_entity) for param_entity in request_entity.data[1:]]

    if cmd == "ECHO":
        return (
            RedisEntity.from_data(RedisDataType.BULK_STRING, params[0]).to_buffer(),
            False,
        )

    if cmd == "INFO":
        if params[0] == "replication":
            replica_info = f"role:{cache.env.get('role')}"
            replica_info += f"\nmaster_replid:{cache.env.get('nmaster_replid')}"
            replica_info += (
                f"\nmaster_repl_offset:{cache.env.get('master_repl_offset')}"
            )
            return (
                RedisEntity.from_data(
                    RedisDataType.BULK_STRING, replica_info
                ).to_buffer(),
                False,
            )

    if cmd == "SET":
        expire = -1
        unit = "s"
        if len(params) > 2:
            if params[2].lower() == "px":
                expire = int(params[3])
                unit = "ms"
            else:
                expire = int(params[3])
        cache.set(params[0], params[1], expire, unit)
        return (
            RedisEntity.from_data(RedisDataType.SIMPLE_STRING, "OK").to_buffer(),
            False,
        )

    if cmd == "GET":
        value = cache.get(params[0])
        if value:
            return (
                RedisEntity.from_data(
                    RedisDataType.BULK_STRING, value.value
                ).to_buffer(),
                False,
            )
        else:
            return (
                RedisEntity.from_data(RedisDataType.BULK_STRING, None).to_buffer(),
                False,
            )

    if cmd == "DEL":
        cache.delete(params[0])
        return (
            RedisEntity.from_data(RedisDataType.SIMPLE_STRING, "OK").to_buffer(),
            False,
        )

    if cmd == "KEYS":
        keys = cache.keys(params[0])
        return (RedisEntity.from_data(RedisDataType.ARRAY, keys).to_buffer(), False)

    if cmd == "PING":
        print(RedisEntity.from_data(RedisDataType.SIMPLE_STRING, "PONG"))
        return (
            RedisEntity.from_data(RedisDataType.SIMPLE_STRING, "PONG").to_buffer(),
            False,
        )

    if cmd == "CONFIG":
        if params[0] == "GET":
            return (
                RedisEntity.from_data(
                    RedisDataType.ARRAY, [params[1], cache.get_config(params[1])]
                ).to_buffer(),
                False,
            )

    if cmd == "REPLCONF":
        return (
            RedisEntity.from_data(RedisDataType.SIMPLE_STRING, "OK").to_buffer(),
            False,
        )

    if cmd == "PSYNC":
        rdb = RedisRDBFile()
        return (
            [
                RedisEntity.from_data(
                    RedisDataType.SIMPLE_STRING,
                    f"FULLRESYNC {cache.env.get('replid')} 0",
                ).to_buffer(),
                RedisEntity.from_data(
                    RedisDataType.BULK_STRING, rdb.to_bytes()
                ).to_buffer(),
            ],
            False,
        )

    return (
        RedisEntity.from_data(
            RedisDataType.SIMPLE_ERROR, "Unknown command"
        ).to_buffer(),
        False,
    )


def init_replica(env):
    replica_port = env.get("port")
    master_host, master_port = env.get("replicaof")
    print(f"Connecting to replica on port {master_host}:{master_port}")
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master_socket.connect((master_host, master_port))

    master_socket.send(RedisEntity.from_data(RedisDataType.ARRAY, ["PING"]).to_buffer())
    response = master_socket.recv(1024)
    print(f"Master: {response}")

    master_socket.send(
        RedisEntity.from_data(
            RedisDataType.ARRAY,
            ["REPLCONF", "listening-port", str(replica_port)],
        ).to_buffer()
    )
    response = master_socket.recv(1024)
    print(f"Master: {response}")

    master_socket.send(
        RedisEntity.from_data(
            RedisDataType.ARRAY,
            ["REPLCONF", "capa", "eof"],
        ).to_buffer()
    )
    response = master_socket.recv(1024)
    print(f"Master: {response}")

    master_socket.send(
        RedisEntity.from_data(
            RedisDataType.ARRAY,
            ["PSYNC", "?", "-1"],
        ).to_buffer()
    )
    response = master_socket.recv(1024)
    print(f"Master: {response}")
