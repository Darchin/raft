import threading
import time

class Node():
    # Time between every heartbeat in seconds
    Heartbeat_period = 0.1
    def __init__(self):

        threading.Thread(target=self.periodic_heartbeat, args=self)
        pass

    def periodic_heartbeat(self):
        while self._role == 'leader':
            # send empty log to all other nodes
            time.sleep(0.1)