from ..messages.message import *
import threading
import time

class State():
    Follower = 1
    Candidate = 2
    Leader = 3
    # 'log' is a list of tuples. tuple pos0 is entry term and tuple pos1 is the entry itself.
    def __init__(self, server, current_term = 0, voted_for = set([None]), log = [], commit_length = 0,
                 current_role = Follower, current_leader = None, votes_received = set(), sent_length = {}, acked_length = {}):
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
        
        # Implementation specific variables used to facilitate certain events and calculations
        self.server = server
        self.node_majority = int(len(self.server._node_ids)/2) + 1
        self.id = server._id
        self.should_cancel_election = False
        self.heartbeat_period = 0.1
    
    # def assign_to_server(self, server):
    #     self.server = server

    def handle_received_message(self, message):
        match message._type:
            case Message.Broadcast: self.on_broadcast(message)
            case Message.LogRequest: self.on_log_request(message)
            case Message.LogResponse: self.on_log_response(message)
            case Message.VoteRequest: self.on_vote_request(message)
            case Message.VoteResponse: self.on_vote_response(message)
    
    def send_message(self, recipient, message):
        pass

    def commit_log_entries(self):
        def acks(length):
            ack_nodes = set()
            for node in self.server._node_ids:
                if self._acked_length[node] >= length:
                    ack_nodes.add(node)
            return len(ack_nodes)
        min_acks = self.node_majority
        ready = set([l for l in range(1, len(self._log)+1) if acks(l) >= min_acks])
        highest_rdy_to_commit = max(ready)
        if len(ready) != 0 and highest_rdy_to_commit > self._commit_length and self._log[highest_rdy_to_commit - 1][0] == self._current_term:
            for i in range(self._commit_length, highest_rdy_to_commit):
                self.deliver_to_server(self._log[i][1])
            self._commit_length = highest_rdy_to_commit

    def deliver_to_server(self, instruction):
        self.server._instructions.append(instruction)

    def append_entries(self, prefix_len, leader_commit, suffix):
        suffix_len = len(suffix)
        if suffix_len > 0 and len(self._log) > prefix_len:
            index = min(len(self._log), prefix_len + suffix_len) - 1
            if self._log[index][0] != suffix[index-prefix_len][0]:
                self._log = self._log[:prefix_len]
        
        if prefix_len + suffix_len > len(self._log):
            for i in range(len(self._log) - prefix_len, suffix_len):
                self._log.append(suffix[i])
        
        if leader_commit > self._commit_length:
            for i in range(self._commit_length, leader_commit):
                self.deliver_to_server(self._log[i][1])
            self._commit_length = leader_commit
    
    def replicate_log(self, follower_id):
        prefix_len = self._sent_length[follower_id]
        suffix = self._log[prefix_len:-1]
        prefix_term = 0
        if prefix_len > 0: prefix_term = self._log[prefix_len - 1][0]

        msg_to_send = LogRequest(self.id, self._current_term, prefix_len, prefix_term, self._commit_length, suffix)
        self.send_message(follower_id, msg_to_send)

    def on_broadcast(self, message):
        record = message._record

        if self._current_role == State.Leader:
            self._log.append((self._current_term, record))
            self._acked_length[self.id] = len(self._log)
            for follower_node in (self._node_ids - set([self._id])):
                self.replicate_log(follower_node)
        else:
            # Forward client request to leader if received by a follower
            self.send_message(self._current_leader, message)

    def on_log_request(self, message):
        leader_id = message.leader_id
        term = message.term
        prefix_len = message.prefix_len
        prefix_term = message.prefix_term
        leader_commit = message.leader_commit
        suffix = message.suffix
    
        if term > self._current_term:
            self._current_term = term
            self._voted_for = set([None])
            self.cancel_election()
        
        if term == self._current_term:
            self._role = State.Follower
            self._current_leader = leader_id

        log_ok = (len(self._log) >= prefix_len) and (prefix_len == 0 or self._log[prefix_len - 1] == prefix_term)

        if term == self._current_term and log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            ack = prefix_len + len(suffix)
            msg_to_send = Message(Message.LogResponse, node_id = self.server._id, current_term = self._current_term,
                                  ack = ack, success = True)
        else:
            msg_to_send = Message(Message.LogResponse, node_id = self.server._id, current_term = self._current_term,
                                  ack = 0, success = False)
        self.send_message(leader_id, msg_to_send)
    
    def on_log_response(self, message):
        follower_id = message.node_id
        term = message.term
        ack = message.ack
        success = message.success

        if term == self._current_term and self._current_role == State.Leader:
            if success == True and ack >= self._acked_length[follower_id]:
                self._sent_length[follower_id] = ack
                self._acked_length[follower_id] = ack
                self.commit_log_entries()
            elif self._sent_length[follower_id] > 0:
                self._sent_length[follower_id] -= 1
                self.replicate_log(follower_id)
        elif term > self._current_term:
            self._current_term = term
            self._current_role = State.Follower
            # self.change_role(State.Follower)
            self._voted_for = set([None])
            self.cancel_election()
    
    def on_vote_request(self, message):
        candidate_id = message.candidate_id
        candidate_term = message.candidate_term
        candidate_log_length = message.candidate_log_length
        candidate_log_term = message.candidate_log_term
        vote_granted = False

        if candidate_term > self._current_term:
            self._current_term = candidate_term
            self.change_role(State.Follower)
            self._voted_for = set([None])

        last_term = self._log[-1][0] if len(self._log) > 0 else 0

        log_ok = (candidate_log_term > last_term) or (candidate_log_term == last_term and candidate_log_length >= len(self._log))

        if candidate_term == self._current_term and log_ok and self._voted_for.issubset(set([None, candidate_id])):
            self._voted_for = set([candidate_id])
            vote_granted = True
        msg_to_send = VoteResponse(self.id, self._current_term, vote_granted)
        self.send_message(candidate_id, msg_to_send)

    def on_vote_response(self, message):    
        voter_id = message.voter_id
        term = message.term
        granted = message.granted

        if self._current_role == State.Candidate and term == self._current_term and granted:
            self._votes_received.union(set([voter_id]))
            if len(self._votes_received) >= self.node_majority:
                self.change_role(State.Leader)
                self._current_leader = self.id
                self.cancel_election()
                
                for follower_node in (self._node_ids - set([self._id])):
                    self._sent_length[follower_node] = len(self._log)
                    self._acked_length[follower_node] = 0
                    self.replicate_log(follower_node)
                
        elif term > self._current_term:
            self._current_term = term
            self.change_role(State.Follower)
            self._voted_for = set([None])
            self.cancel_election()

    def cancel_election():
        should_cancel_election = True
    
    def change_role(self, new_role):
        if new_role == State.Leader:
            threading.Thread(target=self.periodic_heartbeat, args=self).start()

    def hold_election():
        pass
    
    def periodic_heartbeat(self):
        while self._current_role == State.Leader:
            for node in (self._node_ids - set([self._id])):
                self.replicate_log(node)
            time.sleep(self.heartbeat_period)