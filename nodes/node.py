from ..states.state import State
# from ..messages.message import *
import socket
import threading
import sys

class Node():
    
    def __init__(self, id, node_ids: set, instructions = []):
        self._id = id
        self._node_ids = node_ids
        self._state = State(server=self)
        self._instructions = instructions
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(('127.0.0.1', 9999))
        th = threading.Thread(target=self.listen_to_server, args=tuple())
        th.start()

    def listen_to_server(self):
        while True:
            response = self.client_socket.recv(1024).decode()
            print(f"Response from server: {response}")

    def send_message_to_another_node(self, message):
        self.client_socket.sendall(message.encode())
        
        # Receive response from server
        # response = self.client_socket.recv(1024).decode()
        # print(f"Response from server: {response}")

def main():
    Node(sys.argv[1], set())
if __name__=='__main__':
    main()