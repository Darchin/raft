class Message:
    # Usage of 'Message' parent class allows addition of certain global features like timestamps.
    Override = -1
    Broadcast = 0
    LogRequest = 1
    LogResponse = 2
    VoteRequest = 3
    VoteResponse = 4

class Override(Message):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Broadcast(Message):
    def __init__(self, record):
        self._record = record

class LogRequest(Message):
    def __init__(self, leader_id, term, prefix_len, prefix_term, leader_commit, suffix):
        self._leader_id = leader_id
        self._term = term
        self._prefix_len = prefix_len
        self._prefix_term = prefix_term
        self._leader_commit = leader_commit
        self._suffix = suffix
        
class LogResponse(Message):
    def __init__(self, follower_id, term, ack, success):
        self._type = Message.LogResponse
        self._follower_id = follower_id
        self._term = term
        self._ack = ack
        self._success = success


class VoteRequest(Message):
    def __init__(self, candidate_id, candidate_term, candidate_log_length, candidate_log_term):
        self._candidate_id = candidate_id
        self._candidate_term = candidate_term
        self._candidate_log_length = candidate_log_length
        self._candidate_log_term = candidate_log_term

class VoteResponse(Message):
    def __init__(self, voter_id, term, granted):
        self._voter_id = voter_id
        self._term = term
        self._granted = granted