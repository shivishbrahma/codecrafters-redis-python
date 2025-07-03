import socket  # noqa: F401
import threading
from .pub_server import handle_request

def send_request(client_socket: socket.socket, addr):
    close = False
    while not close:
        req_buff = client_socket.recv(1024)
        print(f"Received request from {addr}")
        if req_buff is None or len(req_buff) == 0:
            break

        resp, close = handle_request(req_buff)
        client_socket.sendall(resp)
    print("Client closed connection")
    client_socket.close()

def start_server(server_socket: socket.socket):
    client_socket = None
    while True:
        client_socket, addr = server_socket.accept()  # wait for client
        print(f"Connected to {addr}")
        threading.Thread(target=send_request, args=(client_socket, addr)).start()

def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    print(f"Server started on {server_socket.getsockname()}")
    try:
        start_server(server_socket)
    except KeyboardInterrupt:
        print("\nServer is shutting down...")


if __name__ == "__main__":
    main()
