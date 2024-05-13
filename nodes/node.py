from ..states.state import State
from ..messages.message import *

class Node():
    # Time between every heartbeat in seconds
    Heartbeat_period = 0.1
    def __init__(self, id, node_ids: set):
        self._id = id
        self._node_ids = node_ids
        self._state = State(server=self)
        # self._state.assign_to_server(self)
