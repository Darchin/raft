class Message:
    # Usage of 'Message' parent class allows addition of certain global features like timestamps.
    Override = -1
    ClientRequest = 0
    LogRequest = 1
    LogResponse = 2
    VoteRequest = 3
    VoteResponse = 4

    def __init__(self, sender_id, type):
        self.sender_id: int = sender_id
        self.type: int = type

    def to_string(self):
        """Converts the Message object to a formatted string."""
        attributes = [f"{getattr(self, attr)}" for attr in self.__dict__]
        return f"{self.__class__.__name__}, {', '.join(attributes)}"
    
    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        parts = message_string.split(", ")
        message_type = parts[0]  # Assuming the first part is the message type

        if message_type == "ClientRequest":
            return ClientRequest.from_string(message_string)
        
        elif message_type == "LogRequest":
            return LogRequest.from_string(message_string)
        
        elif message_type == "LogResponse":
            return LogResponse.from_string(message_string)
        
        elif message_type == "VoteRequest":
            return VoteRequest.from_string(message_string)
        
        elif message_type == "VoteResponse":
            return VoteResponse.from_string(message_string)
        
        # If the message type is not recognized, return None or raise an exception
        return None


class Override(Message):
    def __init__(self, **kwargs):
        super(Override, self).__init__(0, Message.Override)
        self.__dict__.update(kwargs)    

class ClientRequest(Message):
    def __init__(self, instruction_to_add:str):
        super(ClientRequest, self).__init__(0, Message.ClientRequest)
        self.instruction_to_add = instruction_to_add

    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        parts = message_string.split(", ")
        instruction_to_add = parts[1]
        return ClientRequest(instruction_to_add)

class LogRequest(Message):
    def __init__(self, leader_id:int, term:int, prefix_len:int, prefix_term:int, leader_commit:int, suffix:list[tuple[int, str]]):
        super(LogRequest, self).__init__(leader_id, Message.LogRequest)
        self.leader_id = leader_id
        self.term = term
        self.prefix_len = prefix_len
        self.prefix_term = prefix_term
        self.leader_commit = leader_commit
        self.suffix = suffix

    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        # print(message_string)
        parts = message_string.split(", ")
        leader_id = int(parts[1])
        term = int(parts[2])
        prefix_len = int(parts[3])
        prefix_term = int(parts[4])
        leader_commit = int(parts[5])
        suffix = list(parts[6])
        print("::::::::::", type(suffix), suffix, leader_id, leader_commit, term, prefix_term)
        # if len(suffix):
        #     print(type(suffix[0]), type(suffix[0][0]), suffix[0])
        return LogRequest(leader_id, term, prefix_len, prefix_term, leader_commit, suffix)
        
class LogResponse(Message):
    def __init__(self, follower_id:int, term:int, ack:int, success:bool):
        super(LogResponse, self).__init__(follower_id, Message.LogResponse)
        self.follower_id = follower_id
        self.term = term
        self.ack = ack
        self.success = success

    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        parts = message_string.split(", ")
        follower_id = int(parts[1])
        term = int(parts[2])
        ack = int(parts[3])
        success = bool(parts[4])
        return LogResponse(follower_id, term, ack, success) 

class VoteRequest(Message):
    def __init__(self, candidate_id:int, candidate_term:int, candidate_log_length:int, candidate_log_term:int):
        super(VoteRequest, self).__init__(candidate_id, Message.VoteRequest)
        self.candidate_id = candidate_id
        self.candidate_term = candidate_term
        self.candidate_log_length = candidate_log_length
        self.candidate_log_term = candidate_log_term

    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        parts = message_string.split(", ")
        candidate_id = int(parts[1])
        candidate_term = int(parts[2])
        candidate_log_length = int(parts[3])
        candidate_log_term = int(parts[4])
        return VoteRequest(candidate_id, candidate_term, candidate_log_length, candidate_log_term)

class VoteResponse(Message):
    def __init__(self, voter_id:int, current_term:int, granted:bool):
        super(VoteResponse, self).__init__(voter_id, Message.VoteResponse)
        self.voter_id = voter_id
        self.current_term = current_term
        self.granted = granted

    @classmethod
    def from_string(cls, message_string):
        """Creates a Message object from a formatted string."""
        parts = message_string.split(", ")
        voter_id = int(parts[1])
        current_term = int(parts[2])
        granted = bool(parts[3])
        return VoteResponse(voter_id, current_term, granted)
