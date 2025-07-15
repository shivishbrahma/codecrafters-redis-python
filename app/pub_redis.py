from enum import Enum
import threading
import os
import struct
import re
import time

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


class CacheValue:
    def __init__(self, value, expired_at, unit="s"):
        self.value = value
        self.expired_at = expired_at
        self.unit = unit

    def is_expired(self):
        if self.expired_at == -1:
            return False
        current_time = time.time()
        expire_in_s = self.expired_at if self.unit == "s" else self.expired_at / 1000
        return current_time > expire_in_s

    def __str__(self):
        return f"{self.value}"

    def __repr__(self):
        return f"CacheValue(value={self.value}, expired_at={self.expired_at}, unit={self.unit})"


class RDBFile:
    def __init__(self, filename):
        self.__filename = filename
        self.__version = 11
        self.__metadata = {}

    def __read_rdb_header(self, file):
        # Header section
        header = file.read(9)
        if not header.startswith(b"REDIS"):
            raise Exception("Invalid RDB file")
        self.__version = int(header[-4:].decode())

    def __read_length(self, file):
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
                    data[key] = CacheValue(val, -1)
                    continue

                elif opcode == 0xFC:  # EXPIRETIMEMS
                    expired_at = self.__read_integer(f, 3)
                    data_type = f.read(1)[0]
                    key = self.__read_string(f)
                    val = self.__read_string(f)
                    data[key] = CacheValue(val, expired_at=expired_at, unit="ms")
                    continue

                elif opcode == 0xFD:  # EXPIRETIME
                    expired_at = self.__read_integer(f, 2)
                    data_type = f.read(1)[0]
                    key = self.__read_string(f)
                    val = self.__read_string(f)
                    data[key] = CacheValue(val, expired_at=expired_at)
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


class Cache:
    def __init__(self, dir=None, dbfilename="dump.rdb"):
        self.__dir = dir if dir else os.getcwd()
        self.__dbfilename = dbfilename
        self.__rdb = RDBFile(os.path.join(self.__dir, self.__dbfilename))
        self.__cache = {}

        loaded_cache = self.__rdb.read()
        if loaded_cache:
            for key, val in loaded_cache.items():
                self.__cache[key] = val
            print(self.__cache)
            print("Cache loaded from RDB file")
        else:
            print("Cache initialized from scratch")

    def set(self, key: str, value: str, expire: float = -1, unit="s"):
        current_time = time.time()
        if expire > 0:
            expired_at = (current_time * 1000 if unit == "ms" else current_time) + expire
        else:
            expired_at = -1

        self.__cache[key] = CacheValue(
            value,
            expired_at,
            unit,
        )

    def get(self, key: str) -> CacheValue | None:
        if key not in self.__cache:
            return None
        if self.__cache[key].is_expired():
            self.delete(key)
            return None
        return self.__cache.get(key)

    def delete(self, key: str):
        self.__cache.pop(key, None)

    def get_config(self, key: str):
        if key == "dir":
            return self.__dir
        elif key == "dbfilename":
            return self.__dbfilename
        else:
            return None

    def save(self):
        try:
            self.__rdb.write()
        except Exception as e:
            print(f"Error saving RDB file: {e}")
            return False

        return True

    def keys(self, pattern: str):
        if pattern:
            pat = re.compile(pattern=pattern.replace("*", ".*"))
            return [key for key in self.__cache.keys() if pat.findall(key)]

        return list(self.__cache.keys())


# if __name__ == "__main__":
#     rdb = RDBFile("/Users/anitesh/Documents/Practice/dump.rdb")
