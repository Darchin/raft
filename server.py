import zmq
from states.state import State
from states.follower import Follower
from states.candidate import Candidate
from states.leader import Leader
from messages.message import *
from memoryBoard.memory_board import MemoryBoard
from utils.json_tools import read_from_json
import sys
import socket
import threading
import time
import os
from typing import TypeVar
ArbitraryState = TypeVar('ArbitraryState', bound=State)

class Server:
    BasePort = 15000
    def __init__(self, id, messageBoard):
        self.id = id
        self._messageBoard = messageBoard
        self.port = Server.BasePort + int(self.id)
        self.node_ids = set(range(0,3))
        self.state: ArbitraryState
        self.instructions = []
        self.create_state(Follower)
        
        class SubscribeThread(threading.Thread):
            def run(thread):
                context = zmq.Context()
                socket = context.socket(zmq.SUB)
                for n in self.node_ids:
                    socket.connect("tcp://127.0.0.1:%d" % (Server.BasePort + int(n)))
                
                topic_filter = str(self.id)
                socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
                while True:
                    message = str(socket.recv())[2:-1]
                    recipient = int(message[:1])
                    message = message[2:]
                    # sender = message[-1:]
                    # print(message, "::" , sender,  " : " , self.id)
                    # continue
                    if self.id == recipient:
                        self.on_message(Message.from_string(message))
                    else:
                        print("bullshit")

        class PublishThread(threading.Thread):
            def run(thread):
                context = zmq.Context()
                socket = context.socket(zmq.PUB)
                socket.bind("tcp://127.0.0.1:%d" % self.port)

                while True:
                    recipient, message = self._messageBoard.get_message()
                    if not message:
                        continue # sleep wait?
                    # print(self.id, "  : " ,  recipient, message)
                    socket.send_string(f"{recipient} {message.to_string()}")
                    # socket.send_string(f"{recipient} salam {self.id}")


        self.subscribeThread = SubscribeThread()
        self.publishThread = PublishThread()

        self.subscribeThread.daemon = True
        self.subscribeThread.start()
        self.publishThread.daemon = True
        self.publishThread.start()
        print(self)
        
    def __str__(self) -> str:
        string_to_print = f'\nServer {self.id}\'s loaded state parameters:\n' \
              f'\t - Current term = {self.state.current_term}\n' \
              f'\t - Voted = {self.state.voted}\n' \
              f'\t - Log entries = {self.state.log}\n' \
              f'\t - Commit length = {self.state.commit_length}\n'
        return string_to_print
    
    def create_state(self, StateOfType: ArbitraryState):
        var_dict = read_from_json(f'./stored_data/{self.id}.json')
        if len(var_dict) == 0:
            self.state = StateOfType(self, current_term=0, voted=False, log=[], commit_length=0)
        else:
            self.state = StateOfType(self, var_dict['current_term'], var_dict['voted'], var_dict['log'], var_dict['commit_length'])
        self.state.create_and_start_threads()
            
    def change_state(self, state_to_change_into):
        self.state.running_thread.stop()
        # self.state.running_thread.join()
        # del self.state.running_thread
        match state_to_change_into:
            case State.Follower: self.create_state(Follower)
            case State.Candidate: self.create_state(Candidate)
            case State.Leader: self.create_state(Leader)

   
    def send_message(self,recipient , message):
        self.post_message(recipient, message)

    def post_message(self,recipient, message):
        self._messageBoard.post_message(recipient, message)

    def on_message(self, message):
        self.state.handle_received_message(message)

def main():
    server_id = int(sys.argv[1])
    os.system(f"title Server {server_id}")
    if len(sys.argv) == 3:
        clean_start = True if sys.argv[2] == 'True' else False
    if clean_start:
        try: 
            os.remove(f'./stored_data/{server_id}.json')
            print(f"[Log]: Prior data deleted.")
        except FileNotFoundError: print(f"[Log]: No prior data found.")
    MB = MemoryBoard()
    Server(server_id, messageBoard = MB)
    while True: pass
    # print(type(server.state))

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)