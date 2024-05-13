class Message:
    Generic = 0
    LogRequest = 1
    LogResponse = 2
    VoteRequest = 3
    VoteResponse = 4

    def __init__(self, type, **kwargs):
        self._type = type

        match type:
            
            case Message.Generic:
                self.__dict__.update(kwargs)

            case Message.LogRequest:
                self.leader_id = kwargs['leader_id']
                self.term = kwargs['term']
                self.prefix_len = kwargs['prefix_len']
                self.prefix_term = kwargs['prefix_term']
                self.leader_commit = kwargs['leader_commit']
                self.suffix = kwargs['suffix']
            
            case Message.LogResponse:
                self.follower = kwargs['follower']
                self.term = kwargs['term']
                self.ack = kwargs['ack']
                self.success = kwargs['success']

            case Message.VoteRequest:
                self.candidate_id = kwargs['candidate_id']
                self.candidate_term = kwargs['candidate_term']
                self.candidate_log_length = kwargs['candidate_log_length']
                self.candidate_log_term = kwargs['candidate_log_term']
            
            case Message.VoteResponse:
                self.voter_id = kwargs['voter_id']
                self.term = kwargs['term']
                self.granted = kwargs['granted']