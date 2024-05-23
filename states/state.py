from messages.message import *
from random import randrange
import time
from utils.stoppable_thread import StoppableThread
from utils.json_tools import write_to_json
# from threading import Thread

class State:
    Follower = 1
    Candidate = 2
    Leader = 3

    def __init__(self, server, current_term, voted, log, commit_length):
        
        # To be stored to or restored from stable storage (ie HDD, SSD)
        self.current_term: int = current_term
        self.voted: bool = voted
        self.log: list[tuple] = log
        self.commit_length: int = commit_length

        # Ok to be stored on volatile storage
        self.current_role: int
        
        # Auxilliary
        self.server = server
        self.node_majority: int = 3
        self.id: int = server.id
        self.node_ids: set[int] = set([node_id for node_id in self.server.node_ids])
        self.node_ids_excluding_self: set[int] = self.node_ids - set([self.id])
        self.running_thread: StoppableThread

    # Each state has unique version of this
    def create_and_start_threads(self):
        pass

    def handle_received_message(self, message):
        match message.type:
            case Message.Override: self.set_params(message)
            case Message.ClientRequest: self.on_client_request(message)
            case Message.LogRequest: self.on_log_request(message)
            case Message.LogResponse: self.on_log_response(message)
            case Message.VoteRequest: self.on_vote_request(message)
            case Message.VoteResponse: self.on_vote_response(message)
    
    def send_message(self, recipient, message):
        self.server.send_message(recipient, message)

    # Deliver instruction that is commited on the state machine's log to the host server.
    def deliver_to_server(self, instruction):
        self.server.instructions.append(instruction)

    def set_params(self, message: Override):
        self.__dict__.update(message.__dict__)
        write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})

    # leader different behaviour
    def on_client_request(self, message: ClientRequest):
        # Forward client request to the leader if received by someone other than a leader
        self.send_message(self.current_leader, message)

    # followers and candidates
    def on_log_request(self, message: LogRequest):
        # The leader ID is included so that the follower can update its _current_leader if it is outdated
        leader_id = message.leader_id
        term = message.term
        prefix_len = message.prefix_len
        prefix_term = message.prefix_term
        leader_commit = message.leader_commit
        suffix = message.suffix

        # The receiver checks if the leader's term is at least as itself's.
        # Also, if the receiver happened to be a candidate at the time it received this message, it will cancel its election
        # and revert back to a follower. 
        if term >= self.current_term:
            self.current_leader = leader_id
            self.current_term = term
            if self.current_role == State.Follower:
                # Since a state that is already a follower is not resetting its vote through change_role
                # and only resetting its timeout, its vote needs to be reset.
                self.voted = False
                self.reset_leader_timeout()
            else:
                self.change_role(State.Follower)
        else: 
            self.send_message(leader_id, LogResponse(self.id, self.current_term, 0, False))
            return

        # The leader has sent this log request assuming that the receiver has a log at least as long as "prefix_len"
        # So if the receiver doesn't have even this much, then the suffix that the leader has sent cannot be
        # attached to the end of the receiver's log and the leader needs to resend the log request with more elements.
        # Now even if the log length matches, the receiver's log could be inconsistent. To check for this,
        # we just need to check the last log entry's term, since Raft's implementation guarantees that
        # if two log entries of the same index also have a matching term, then the log up to that point is consistent.
        log_ok = (len(self.log) >= prefix_len) and (prefix_len == 0 or self.log[prefix_len - 1] == prefix_term)

        # If the term and log are consistent, append the entries to the log
        if log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            # 'ack' essentially tells the leader: "Hey I have added every log you have sent up to index 'ack'
            # and I have also committed them up to the commit length you have set."
            ack = prefix_len + len(suffix)
            msg_to_send = LogResponse(self.id, self.current_term, ack, True)
            self.send_message(leader_id, msg_to_send)
        else:
            msg_to_send = LogResponse(self.id, self.current_term, 0, False)
            self.send_message(leader_id, msg_to_send)

    # only follower responds
    def on_vote_request(self, message: VoteRequest):
        # Extract candidate information from the message
        candidate_id = message.candidate_id
        candidate_term = message.candidate_term
        candidate_log_length = message.candidate_log_length
        candidate_log_term = message.candidate_log_term

        # If we are behind the candidate, we will first update our term to match.
        # Also, if we are behind, we need to drop our leadership/election and revert to follower.
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            if self.current_role != State.Follower:
                self.change_role(State.Follower)
            # Get the term of the latest entry in the log, or 0 if the log is empty.
            last_log_entrys_term = self.log[-1][0] if len(self.log) > 0 else 0

            # Check if the candidate's log is at least as up-to-date as ours.
            log_ok = (candidate_log_term == last_log_entrys_term and candidate_log_length >= len(self.log))

            # If the candidate's log passed the check, and we either haven't voted for anyone else this term
            # or we have voted for the same candidate before. Basically we won't change our vote during a term after we cast it.
            # So the first candidate to win our vote has guaranteed it for that term.
            if log_ok and self.voted == False:
                self.voted = True
                write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})
                self.send_message(candidate_id, VoteResponse(self.id, self.current_term, granted=True))
            else: 
                self.send_message(candidate_id, VoteResponse(self.id, self.current_term, granted=False))
        else: 
            self.send_message(candidate_id, VoteResponse(self.id, self.current_term, granted=False))


    # leader doesn't have this
    def append_entries(self, prefix_len, leader_commit, suffix):
        suffix_len = len(suffix)
        
        # If the LogRequest calling this append_entries was just a heartbeat,
        # then suffix should be length zero.
        if suffix_len > 0:
            if prefix_len == 0:
                self.log = suffix
            else:
                self.log = self.log[prefix_len - 1] + suffix
        # Check if the server has allowed more commits.
        if leader_commit > self.commit_length:
            # Deliver the entries to the server starting from the current commit length
            for i in range(self.commit_length, leader_commit):
                self.deliver_to_server(self.log[i][1])
            
            # Update the commit length to match the leader's commit length
            self.commit_length = leader_commit
        write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})
    # exclusive to leader
    def commit_log_entries(self):
        def acks(length):
            ack_nodes = set()
            for node in self.node_ids:
                if self.acked_lengths[node] >= length:
                    ack_nodes.add(node)
            return len(ack_nodes)
        
        min_acks = self.node_majority
        # The minimum number of acknowledgments required for a log entry to be considered acknowledged by a majority of nodes.

        ready = set([l for l in range(self.commit_length, len(self.log)+1) if acks(l) >= min_acks])
        # The ready set contains the indices of log entries that have received acknowledgments from a majority of nodes.
        # It uses a list comprehension to iterate over the range of log entry indices and checks if the number of acknowledgments
        # for each log entry is greater than or equal to the minimum required acknowledgments.

        highest_rdy_to_commit = max(ready) if len(ready) != 0 else 0
        # The highest_rdy_to_commit variable stores the index of the highest log entry that is ready to be committed.

        if highest_rdy_to_commit > self.commit_length and self.log[highest_rdy_to_commit - 1][0] == self.current_term:
            # This condition checks if there are any log entries ready to be committed, if the highest ready log entry index is greater
            # than the current commit length, and if the term of the highest ready log entry matches the current term.

            for i in range(self.commit_length, highest_rdy_to_commit):
                # This loop iterates over the log entries starting from the current commit length up to the highest ready log entry index.
                # It delivers each log entry to the server by calling the deliver_to_server method with the corresponding entry value.

                self.deliver_to_server(self.log[i][1])

            self.commit_length = highest_rdy_to_commit
            write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})
            # After committing the log entries, the commit length is updated to the index of the highest ready log entry.
    
    def change_role(self, state_to_change_into):
        self.server.change_state(state_to_change_into)