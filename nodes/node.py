from concurrent.futures import ThreadPoolExecutor
from ..states.state import State
# from ..messages.message import *
import socket
import threading
import sys
import grpc
import threading
import grpc

class Node():
   
    def __init__(self, id, node_ids: dict, instructions = []):
        self._id = id
        self._node_ids = node_ids
        self._state = State(server=self)
        self._instructions = instructions
        self.server = grpc.server(thread_pool=ThreadPoolExecutor(max_workers=10))
        self.server.add_insecure_port('[::]:50051')
        self.server.start()

    def get_node_connection_details(self, node_id):
            # Assuming you have a field called node_details that keeps the connection details of each node
            if node_id in self._node_ids:
                return self._node_ids[node_id]
            else:
                return None
    def listen_to_server(self):
        while True:
            message = self._state.receive_message()
            if message is not None:
                self._state.handle_message(message)

    def send_message_to_another_node(self, recipient, message):
        recipient_node = self.server.get_node_connection_details(recipient)
        
        if recipient_node is not None:
            with grpc.insecure_channel(recipient_node) as channel:
                stub = YourGRPCStub(channel)
                response = stub.SendMessage(message)
                

        else:
            print(f"Node {recipient} is not found in the node connection details.")
    

        
def main():
    Node(sys.argv[1], set())

if __name__=='__main__':
    main()
