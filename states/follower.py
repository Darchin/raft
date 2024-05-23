from .state import *
from threading import Thread

class Follower(State):
    LEADER_TIMEOUT_INTERVAL_MIN = 2000
    LEADER_TIMEOUT_INTERVAL_MAX = 3000
    LEADER_TIMEOUT_POLLING_INTERVAL = 50

    def __init__(self, server, current_term, voted, log, commit_length):
        super(Follower, self).__init__(server, current_term, voted, log, commit_length)
        self.current_leader: int
        self.current_role = State.Follower
        
        self.timeout_for_this_term: int = 3000
        self.remaining_timeout: int

    def create_and_start_threads(self):
        self.running_thread = StoppableThread(target=self.is_leader_alive, args=tuple())
        self.running_thread.start()

    def reset_leader_timeout(self):
        print(self.id, " : " , self.current_leader)
        self.remaining_timeout = self.timeout_for_this_term
    
    # exclusive to follower
    def is_leader_alive(self):
        self.timeout_for_this_term = randrange(start=Follower.LEADER_TIMEOUT_INTERVAL_MIN,
                                               stop=Follower.LEADER_TIMEOUT_INTERVAL_MAX,
                                               step=Follower.LEADER_TIMEOUT_POLLING_INTERVAL)
        self.remaining_timeout = self.timeout_for_this_term
        while self.remaining_timeout > 0 and not self.running_thread.stopped():
            self.remaining_timeout -= Follower.LEADER_TIMEOUT_POLLING_INTERVAL
            print(self.remaining_timeout)
            # print(self.remaining_timeout)
            time.sleep(Follower.LEADER_TIMEOUT_POLLING_INTERVAL * (1e-3))

        self.change_role(self.Candidate)