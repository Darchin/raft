from ..states.state import State
from ..messages.message import *

class Node():
    # Time between every heartbeat in seconds
    
    def __init__(self, id, node_ids: set, instructions):
        self._id = id
        self._node_ids = node_ids
        self._state = State(server=self)
        self._instructions = instructions
        # self._state.assign_to_server(self)
