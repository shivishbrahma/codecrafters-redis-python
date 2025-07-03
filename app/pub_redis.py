CLRF = "\r\n"


def build_response():
    resp_buff = bytearray()

    resp_buff.extend(f"+PONG{CLRF}".encode())
    return resp_buff
