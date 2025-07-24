import socket  # noqa: F401
import threading
from .pub_server import handle_request
from .pub_redis import RedisCache, RedisEnvironment
from argparse import ArgumentParser


def send_request(client_socket: socket.socket, addr, cache: RedisCache):
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


def start_server(server_socket: socket.socket, cache: RedisCache):
    client_socket = None
    while True:
        client_socket, addr = server_socket.accept()  # wait for client
        print(f"Connected to {addr}")
        threading.Thread(target=send_request, args=(client_socket, addr, cache)).start()


def main():
    parser = ArgumentParser()
    parser.add_argument("--dir", type=str, help="Directory to store data")
    parser.add_argument("--dbfilename", type=str, help="DB file name to store data")
    parser.add_argument("--port", type=int, default=6379, help="Port to listen on")
    parser.add_argument("--replicaof", type=str, help="Replica of another server")

    args = parser.parse_args()

    env = RedisEnvironment()
    if args.port is not None:
        env.set("port", args.port)
    if args.replicaof is not None:
        print("Replica: ", args.replicaof)
        env.set("replicaof", args.replicaof)
    if args.dir is not None:
        env.set("dir", args.dir)
    if args.dbfilename is not None:
        env.set("dbfilename", args.dbfilename)

    cache = RedisCache(env=env)

    port = args.port

    if env.get("replicaof") is not None:
        replica_host, replica_port = env.get("replicaof")
        print(f"Connecting to replica on port {replica_host}:{replica_port}")

    server_socket = socket.create_server(("localhost", port), reuse_port=True)
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
