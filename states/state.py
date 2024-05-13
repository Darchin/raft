from ..messages.message import Message
import threading

class State():
    Follower = 1
    Candidate = 2
    Leader = 3

    def __init__(self, server, current_term = 0, voted_for = None, log = [], commit_length = 0,
                 current_role = Follower, current_leader = None, votes_received = set(), sent_length = [], acked_length = []):
        
        # These variables must be stored on stable storage - ie an HDD/SSD.
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._commit_length = commit_length

        # These variables can be stored on volatile storage.
        self._current_role = current_role
        self._current_leader = current_leader
        self._votes_received = votes_received
        self._sent_length = sent_length
        self._acked_length = acked_length
        
        # Implementation specific variables used to facilitate certain events
        self.server = server
        self.cancelElection = False

    def on_message_received(self, message: Message):
        match message._type:
            case Message.Generic:
                pass
            case Message.LogRequest: self.on_log_request(message)
            case Message.LogResponse:
                pass
            case Message.VoteRequest:
                pass
            case Message.VoteResponse:
                pass
        
    def on_log_request(self, message):
        leader_id = message.leader_id
        term = message.term
        prefix_len = message.prefix_len
        prefix_term = message.prefix_term
        leader_commit = message.leader_commit
        suffix = message.suffix
    
        if term > self._current_term:
            self._current_term = term
            self._voted_for = None
            self.cancel_election()
        
        if term == self._current_term:
            self._role = State.Follower
            self._current_leader = leader_id

        log_ok = (len(self._log) >= prefix_len) and (prefix_len == 0 or self._log[prefix_len - 1] == prefix_term)

        if term == self._current_term and log_ok:
            self.append_entries()

    
    def append_entries(self, prefix_len, leader_commit, suffix):
        

    def cancel_election():
        _cancel_election = True
    
    def change_role(self, new_role):
        if new_role == 'leader':
            threading.Thread(target=self._server.periodic_heartbeat, args=self._server)
        pass
    def hold_election():
        pass
