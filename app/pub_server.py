from typing import Tuple
from .pub_redis import build_response

def handle_request(request_buffer: bytes) -> Tuple[bytes, bool]:
    return (build_response(), False)