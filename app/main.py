import socket  # noqa: F401
import threading
from .pub_server import handle_request
from .pub_redis import Cache
from argparse import ArgumentParser

def send_request(client_socket: socket.socket, addr, cache: Cache):
    close = False
    while not close:
        req_buff = client_socket.recv(1024)
        print(f"Received request from {addr}")
        if req_buff is None or len(req_buff) == 0:
            break

        resp, close = handle_request(req_buff, cache)
        client_socket.sendall(resp)
    print("Client closed connection")
    client_socket.close()

def start_server(server_socket: socket.socket, cache: Cache):
    client_socket = None
    while True:
        client_socket, addr = server_socket.accept()  # wait for client
        print(f"Connected to {addr}")
        threading.Thread(target=send_request, args=(client_socket, addr, cache)).start()

def main():
    parser = ArgumentParser()
    parser.add_argument("--dir", type=str, help="Directory to store data")
    parser.add_argument("--dbfilename", type=str, help="DB file name to store data")

    args = parser.parse_args()

    if args.dir is not None:
        cache = Cache(args.dir, args.dbfilename)
    else:
        cache = Cache()

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print(f"Server started on {server_socket.getsockname()}")
    try:
        start_server(server_socket, cache)
    except KeyboardInterrupt:
        print("\nServer is shutting down...")
    finally:
        server_socket.close()
        # cache.save()


if __name__ == "__main__":
    main()
