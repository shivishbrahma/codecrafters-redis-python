from enum import Enum
import threading
import os
import struct
import re
import time

CLRF = "\r\n"


class RedisDataType(Enum):
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


class RedisEntity:
    @staticmethod
    def from_buffer(buff: bytes):
        return RedisEntity(buff)

    @staticmethod
    def from_data(data_type: RedisDataType, data=None):
        if data_type == RedisDataType.ARRAY:
            data = [
                RedisEntity.from_data(RedisDataType.BULK_STRING, ele) for ele in data
            ]
        return RedisEntity(data_type=data_type, data=data)

    def to_buffer(self):
        data_type = self.__data_type
        data = self.__data

        resp_buff = bytearray()
        resp_buff.extend(f"{data_type.value}".encode())

        if data_type == RedisDataType.SIMPLE_STRING:
            resp_buff.extend(f"{data}{CLRF}".encode())
        if data_type == RedisDataType.BULK_STRING:
            resp_buff.extend(str(len(data) if data else -1).encode())
            resp_buff.extend(CLRF.encode())
            if isinstance(data, bytearray):
                resp_buff.extend(data)
            elif data:
                resp_buff.extend(data.encode())
                resp_buff.extend(CLRF.encode())

            # data = f"{CLRF}{data}" if data else ""
            # resp_buff.extend(f"{data_len}{data}{CLRF}".encode())
        if data_type == RedisDataType.ARRAY:
            resp_buff.extend(f"{len(data)}{CLRF}".encode())
            for item in data:
                resp_buff.extend(item.to_buffer())
        return resp_buff

    def __init__(
        self, buff: bytes = None, data_type=RedisDataType.SIMPLE_STRING, data=""
    ):
        self.__data_type = data_type
        self.__data = data
        if buff:
            self.__parse__(buff)

    def __parse__(self, buff: bytes):
        redis_str = buff.decode()
        if redis_str.startswith(RedisDataType.ARRAY.value):
            self.__data_type = RedisDataType.ARRAY
            self.__data = []
            idx = redis_str.find(CLRF)
            len_cmd = int(redis_str[1:idx])
            idx = idx + len(CLRF)
            for i in range(len_cmd):
                n_idx = redis_str.find(CLRF, idx)
                n_idx = redis_str.find(CLRF, n_idx + 1)
                self.__data.append(
                    RedisEntity.from_buffer(redis_str[idx:n_idx].encode())
                )
                idx = n_idx + len(CLRF)
            return
        if redis_str.startswith(RedisDataType.SIMPLE_STRING.value):
            self.__data_type = RedisDataType.SIMPLE_STRING
            self.__data = redis_str.split(CLRF)[1]
            return
        if redis_str.startswith(RedisDataType.INTEGER.value):
            self.__data_type = RedisDataType.INTEGER
            self.__data = int(redis_str.split(CLRF)[1])
            return
        if redis_str.startswith(RedisDataType.BULK_STRING.value):
            self__data_type = RedisDataType.BULK_STRING
            self.__data = redis_str.split(CLRF)[1]
            return

    def __str__(self):
        if isinstance(self.__data, list):
            return f"{[str(item) for item in self.__data]}"
        return f"{self.__data}"

    @property
    def data(self):
        return self.__data


class RedisEnvironment:
    def __init__(self):
        self.__dir = os.getcwd()
        self.__dbfilename = "dump.rdb"
        self.__port = 6379
        self.__role = "master"
        self.__replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.__replicaof = None
        self.__master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        self.__master_repl_offset = 0

    def get(self, key: str):
        if key == "dir":
            return self.__dir
        elif key == "dbfilename":
            return self.__dbfilename
        elif key == "port":
            return self.__port
        elif key == "role":
            return self.__role
        elif key == "master_replid":
            return self.__master_replid
        elif key == "master_repl_offset":
            return self.__master_repl_offset
        elif key == "replicaof":
            return self.__replicaof
        elif key == "replid":
            return self.__replid
        else:
            return None

    def set(self, key: str, value: str):
        if key == "dir":
            self.__dir = value
        elif key == "dbfilename":
            self.__dbfilename = value
        elif key == "port":
            self.__port = int(value)
        elif key == "replicaof":
            host, port = value.strip().split()
            port = int(port)
            self.__role = "slave"
            self.__replicaof = (host, port)
        else:
            return None


class RedisCacheValue:
    def __init__(self, value, expired_at, unit="s"):
        self.value = value
        self.expired_at = expired_at
        self.unit = unit

    def is_expired(self):
        if self.expired_at == -1:
            return False
        current_time = time.time()
        expire_in_s = self.expired_at
        return current_time > expire_in_s

    def __str__(self):
        return f"{self.value}"

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value}, expired_at={self.expired_at}, unit={self.unit})"


class RedisRDBFile:
    def __init__(self, filename=None):
        self.__filename = filename
        self.__version = 11
        self.__metadata = {}

    def __read_rdb_header(self, file) -> None:
        # Header section
        header = file.read(9)
        if not header.startswith(b"REDIS"):
            raise Exception("Invalid RDB file")
        self.__version = int(header[-4:].decode())

    def __read_length(self, file) -> tuple[int, int]:
        first_byte = file.read(1)
        if not first_byte:
            raise ValueError("Unexpected EOF")
        b = first_byte[0]

        type_bits = b >> 6
        if type_bits == 0 or type_bits == 3:
            return (type_bits, b & 0x3F)
        elif type_bits == 1:
            next_byte = file.read(1)[0]
            return (type_bits, ((b & 0x3F) << 8) | next_byte)
        elif type_bits == 2:
            return (type_bits, struct.unpack(">I", file.read(4))[0])

    def __read_integer(self, file, length):
        if length == 0:
            return struct.pack("<B", file.read(1)[0])[0]
        elif length == 1:
            return struct.unpack("<H", file.read(2))[0]
        elif length == 2:
            return struct.unpack("<I", file.read(4))[0]
        elif length == 3:
            return struct.unpack("<Q", file.read(8))[0]
        return None

    def __read_compressed_string(self, file):
        return ""

    def __read_string(self, file):
        type_bits, length = self.__read_length(file)
        if type_bits == 3:
            integer = self.__read_integer(file, length)
            if integer is not None:
                return integer
            return self.__read_compressed_string(file)

        return file.read(length).decode()

    def read(self):
        if not os.path.exists(self.__filename):
            return {}
        data = {}
        with open(self.__filename, "rb") as f:
            # file_content = f.read()
            self.__read_rdb_header(f)

            while True:
                byte = f.read(1)
                if not byte:
                    break  # EOF
                opcode = ord(byte)

                if opcode == 0xFA:  # AUX
                    aux_key = self.__read_string(f)
                    aux_val = self.__read_string(f)
                    self.__metadata[aux_key] = aux_val
                    continue

                elif opcode == 0xFB:  # RESIZEDB
                    self.__metadata["size_of_hash_table"] = self.__read_length(f)[1]
                    self.__metadata["size_of_expiry_hash_table"] = self.__read_length(
                        f
                    )[1]
                    continue

                elif opcode == 0x00:
                    key = self.__read_string(f)
                    val = self.__read_string(f)
                    data[key] = RedisCacheValue(val, -1)
                    continue

                elif opcode == 0xFC:  # EXPIRETIMEMS
                    expired_at = self.__read_integer(f, 3)
                    data_type = f.read(1)[0]
                    key = self.__read_string(f)
                    val = self.__read_string(f)
                    data[key] = RedisCacheValue(
                        val, expired_at=expired_at / 1000, unit="ms"
                    )
                    continue

                elif opcode == 0xFD:  # EXPIRETIME
                    expired_at = self.__read_integer(f, 2)
                    data_type = f.read(1)[0]
                    key = self.__read_string(f)
                    val = self.__read_string(f)
                    data[key] = RedisCacheValue(val, expired_at=expired_at)
                    continue

                elif opcode == 0xFE:  # SELECTDB
                    db_num = self.__read_length(f)[1]
                    self.__metadata["db"] = db_num
                    continue

                elif opcode == 0xFF:  # EOF
                    break

                else:
                    print(f"Unsupported opcode: {hex(opcode)}")
                    break

        print("Metadata: ", self.__metadata)

        return data

    def write(self):
        raise NotImplementedError
        # out = bytearray()
        # # Header section
        # out.extend(f"REDIS{self.__version:04d}".encode())
        # out.append(0xFA)

        # with open(self.__filename, "rb") as f:
        #     f.write(out)

    def to_bytes(self):
        buff = bytearray()
        buff.extend(f"REDIS{self.__version:04d}".encode())
        buff.append(0xFA)
        buff.extend(b"\x09redis-ver\x057.2.0")
        buff.append(0xFA)
        buff.extend(b"\x0aredis-bits\xc0@")
        buff.extend(b"\xfe\x00")
        buff.append(0xFF)
        buff.extend(b"\x00\x00\x00\x00\x00\x00\x00\x00")
        return buff


class RedisCache:
    def __init__(self, env: RedisEnvironment) -> None:
        self.__env = env
        self.__rdb = RedisRDBFile(
            os.path.join(self.__env.get("dir"), self.__env.get("dbfilename"))
        )
        self.__cache = {}

        loaded_cache = self.__rdb.read()
        if loaded_cache:
            for key, val in loaded_cache.items():
                self.__cache[key] = val
            print(self.__cache)
            print("Cache loaded from RDB file")
        else:
            print("Cache initialized from scratch")

    @property
    def env(self) -> RedisEnvironment:
        return self.__env

    def set(self, key: str, value: str, expire: float = -1, unit="s") -> None:
        current_time = time.time()
        if expire > 0:
            expired_at = current_time + (expire / 1000 if unit == "ms" else expire)
        else:
            expired_at = -1

        self.__cache[key] = RedisCacheValue(
            value,
            expired_at,
            unit,
        )

    def get(self, key: str) -> RedisCacheValue | None:
        if key not in self.__cache:
            return None
        if self.__cache[key].is_expired():
            self.delete(key)
            return None
        return self.__cache.get(key)

    def delete(self, key: str):
        self.__cache.pop(key, None)

    def get_config(self, key: str) -> str | None:
        if key == "dir" or key == "dfilename":
            return self.env.get(key=key)
        else:
            return None

    def save(self) -> bool:
        try:
            self.__rdb.write()
        except Exception as e:
            print(f"Error saving RDB file: {e}")
            return False

        return True

    def keys(self, pattern: str) -> list[str]:
        if pattern:
            pat = re.compile(pattern=pattern.replace("*", ".*"))
            return [key for key in self.__cache.keys() if pat.findall(key)]

        return list(self.__cache.keys())


# if __name__ == "__main__":
#     redisEntity = RedisEntity.from_buffer(
#         "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".encode()
#     )
#     print(redisEntity)
