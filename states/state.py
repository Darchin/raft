from ..messages.message import *
import threading
import time
from random import randrange

class State():
    """
    Represents the state of a Raft node in the Raft consensus algorithm.

    Attributes:
        Follower (int): Represents the follower role.
        Candidate (int): Represents the candidate role.
        Leader (int): Represents the leader role.
        HEARTBEAT_INTERVAL (int): The interval in milliseconds between sending heartbeats.
        LEADER_TIMEOUT_INTERVAL_MIN (int): The minimum interval in milliseconds for leader timeout.
        LEADER_TIMEOUT_INTERVAL_MAX (int): The maximum interval in milliseconds for leader timeout.
        LEADER_TIMEOUT_POLLING_INTERVAL (int): The interval in milliseconds for leader timeout polling.
        CANDIDATE_ELECTION_TIMEOUT (int): The election timeout in milliseconds for a candidate.
        CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL (int): The interval in milliseconds for candidate election timeout polling.

    Args:
        server (Server): The server object associated with the state.
        current_term (int): The current term of the node. Defaults to 0.
        voted_for (set): The set of nodes that the node has voted for. Defaults to set([None]).
        log (list): The log entries of the node. Defaults to an empty list.
        commit_length (int): The length of the committed log entries. Defaults to 0.
        current_role (int): The current role of the node. Defaults to Follower.
        current_leader (int): The ID of the current leader. Defaults to None.
        votes_received (set): The set of nodes that have voted for the node. Defaults to an empty set.
        sent_length (dict): The dictionary of sent log lengths for each follower node. Defaults to an empty dictionary.
        acked_length (dict): The dictionary of acknowledged log lengths for each follower node. Defaults to an empty dictionary.
    """
    
    Follower = 1
    Candidate = 2
    Leader = 3

    HEARTBEAT_INTERVAL = 100
    LEADER_TIMEOUT_INTERVAL_MIN = 500
    LEADER_TIMEOUT_INTERVAL_MAX = 1000 
    LEADER_TIMEOUT_POLLING_INTERVAL = 10
    CANDIDATE_ELECTION_TIMEOUT = 250
    CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL = LEADER_TIMEOUT_POLLING_INTERVAL

    def __init__(self, server, current_term = 0, voted_for = set([None]), log = [], commit_length = 0,
                 current_role = Follower, current_leader = None, votes_received = set(), sent_length = {}, acked_length = {}):
        """
        Initializes a new instance of the State class.

        Args:
            server (Server): The server object associated with the state.
            current_term (int): The current term of the node. Defaults to 0.
            voted_for (set): The set of nodes that the node has voted for. Defaults to set([None]).
            log (list): The log entries of the node. Defaults to an empty list.
            commit_length (int): The length of the committed log entries. Defaults to 0.
            current_role (int): The current role of the node. Defaults to Follower.
            current_leader (int): The ID of the current leader. Defaults to None.
            votes_received (set): The set of nodes that have voted for the node. Defaults to an empty set.
            sent_length (dict): The dictionary of sent log lengths for each follower node. Defaults to an empty dictionary.
            acked_length (dict): The dictionary of acknowledged log lengths for each follower node. Defaults to an empty dictionary.
        """
        
        self._current_term = current_term
        self._voted_for = voted_for
        self._log = log
        self._commit_length = commit_length

        self._current_role = current_role
        self._current_leader = current_leader
        self._votes_received = votes_received
        self._sent_length = sent_length
        self._acked_length = acked_length
        
        self.server = server
        self.node_majority = int(len(self.server._node_ids)/2) + 1
        self.id = server._id
        self.cancel_election = False
        self.leader_timeout = 0
        self.election_timeout = 0
        self.node_ids = set([node_id for node_id in self.server._node_ids])
        self.node_ids_excluding_self = self.node_ids - set([self.id])

    def handle_received_message(self, message):
        """
        Handles the received message based on its type.

        Args:
            message: The message object to be handled.

        Returns:
            None
        """
        match message._type:
            case Message.Override: self.set_params(message)
            case Message.Broadcast: self.on_broadcast(message)
            case Message.LogRequest: self.on_log_request(message)
            case Message.LogResponse: self.on_log_response(message)
            case Message.VoteRequest: self.on_vote_request(message)
            case Message.VoteResponse: self.on_vote_response(message)
    
    # TODO
    def send_message(self, recipient, message):
        pass

    def deliver_to_server(self, instruction):
        """
        Delivers an instruction to the server.

        Args:
            instruction (str): The instruction to be delivered.

        Returns:
            None
        """
        self.server._instructions.append(instruction)

    def set_params(self, message):
        """
        Updates the state object with the attributes from the given message object.

        Args:
            message: An object containing the attributes to update the state object with.

        Returns:
            None
        """
        self.__dict__.update(message.__dict__)

    def on_broadcast(self, message):
        """
        Handles the broadcast message received by the state.

        Args:
            message: The broadcast message object.

        Returns:
            None
        """
        record = message._record

        if self._current_role == State.Leader:
            self._log.append((self._current_term, record))
            self._acked_length[self.id] = len(self._log)
            for follower_node in self.node_ids_excluding_self:
                self.replicate_log(follower_node)
        else:
            # Forward client request to leader if received by a follower
            self.send_message(self._current_leader, message)

    def on_log_request(self, message):
        """
        Handles a log request message from the leader.

        Args:
            message (Message): The log request message containing information from the leader.

        Returns:
            None
        """
        leader_id = message.leader_id
        term = message.term
        prefix_len = message.prefix_len
        prefix_term = message.prefix_term
        leader_commit = message.leader_commit
        suffix = message.suffix

        if term > self._current_term:
            self._current_term = term
            self._voted_for = set([None])
            self.cancel_election = True

        if term == self._current_term:
            self._role = State.Follower
            self._current_leader = leader_id

        log_ok = (len(self._log) >= prefix_len) and (prefix_len == 0 or self._log[prefix_len - 1] == prefix_term)

        if term == self._current_term and log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            ack = prefix_len + len(suffix)
            msg_to_send = Message(Message.LogResponse, node_id=self.server._id, current_term=self._current_term,
                                  ack=ack, success=True)
        else:
            msg_to_send = Message(Message.LogResponse, node_id=self.server._id, current_term=self._current_term,
                                  ack=0, success=False)
        self.send_message(leader_id, msg_to_send)
    
    def on_log_response(self, message):
        """
        Process the log response received from a follower.

        Args:
            message: The log response message received from the follower.

        Returns:
            None
        """
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
        """
        Handles a vote request from a candidate.

        Args:
            message: The vote request message containing candidate information.

        Returns:
            None
        """
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
        """
        Process the vote response received from a voter.

        Args:
            message: The vote response message containing voter_id, term, and granted.

        Returns:
            None
        """
        voter_id = message.voter_id
        term = message.term
        granted = message.granted

        if self._current_role == State.Candidate and term == self._current_term and granted:
            self._votes_received.union(set([voter_id]))
            if len(self._votes_received) >= self.node_majority:
                self.cancel_election = True
                # Give enough time to election cancellation status polling to register the change
                time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL/1000)
                self.change_role(State.Leader)
                
        elif term > self._current_term:
            self.cancel_election = True
            # Give enough time to election cancellation status polling to register the change
            time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL/1000)
            self._current_term = term
            self.change_role(State.Follower)
            self._voted_for = set([None])

    # Depends on 'deliver_to_server'
    def append_entries(self, prefix_len, leader_commit, suffix):
        """
        Appends entries to the log and updates the commit length.

        Args:
            prefix_len (int): The length of the prefix in the log.
            leader_commit (int): The commit length of the leader.
            suffix (list): The entries to be appended to the log.

        Returns:
            None
        """
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

    # Depends on 'on_log_request' and 'send_message'
    def replicate_log(self, follower_id):
        """
        Replicates the log to a specific follower.

        Args:
            follower_id (int): The ID of the follower to replicate the log to.

        Returns:
            None
        """
        prefix_len = self._sent_length[follower_id]
        suffix = self._log[prefix_len:-1]
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self._log[prefix_len - 1][0]

        msg_to_send = LogRequest(self.id, self._current_term, prefix_len, prefix_term, self._commit_length, suffix)
        self.send_message(follower_id, msg_to_send)

    # Depends on 'deliver_to_server'
    def commit_log_entries(self):
        """
        Commits log entries that have been acknowledged by a majority of nodes.

        This method checks for log entries that have received acknowledgments from a majority of nodes.
        It then commits those log entries by delivering them to the server.

        Returns:
            None
        """
        def acks(length):
            ack_nodes = set()
            for node in self.node_ids:
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
    
    def change_role(self, new_role):
        """
        Change the role of the Raft node to the specified new_role.

        Args:
            new_role (State): The new role to be assigned to the Raft node.

        Returns:
            None

        Raises:
            None
        """
        self.cancel_election = False
        match new_role:
            case State.Follower:
                self._current_role = State.Follower
                threading.Thread(target=self.leader_check, args=tuple()).start()

            case State.Candidate:
                self._current_role = State.Candidate
                self._voted_for = set([self.id])
                self._votes_received = set([self.id])
                threading.Thread(target=self.hold_election, args=tuple()).start()

            case State.Leader:
                self._current_role = State.Leader
                self._current_leader = self.id
                
                for follower_node in self.node_ids_excluding_self:
                    self._sent_length[follower_node] = len(self._log)
                    self._acked_length[follower_node] = 0
                    self.replicate_log(follower_node)

                threading.Thread(target=self.periodic_heartbeat, args=tuple()).start()

    def hold_election(self):
        """
        Initiates an election process by sending vote requests to other nodes.

        This method sets the election timeout, increments the current term,
        prepares a vote request message, and sends it to all other nodes except self.
        It then waits for the election timeout to expire or until the election is canceled.

        If the election is not canceled, the method recursively calls itself to hold a new election.

        Note: This method assumes the existence of the following attributes:
        - self.election_timeout: The current election timeout value.
        - self._current_term: The current term of the node.
        - self._log: The log of the node.
        - self.id: The ID of the node.
        - self.node_ids_excluding_self: A list of node IDs excluding self.
        - self.send_message(node, message): A method to send a message to a specific node.
        - State.CANDIDATE_ELECTION_TIMEOUT: The election timeout duration.
        - State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL: The polling interval for checking the election timeout.
        - self.cancel_election: A flag to indicate if the election is canceled.
        """
        self.election_timeout = State.CANDIDATE_ELECTION_TIMEOUT
        self._current_term += 1
        last_term = self._log[-1][0] if len(self._log) > 0 else 0

        msg_to_send = VoteRequest(self.id, self._current_term, len(self._log), last_term)
        for node in self.node_ids_excluding_self:
            self.send_message(node, msg_to_send)

        while self.election_timeout > 0 and not self.cancel_election:
            self.election_timeout -= State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL
            # sleeping for slightly less than a millisecond to account for the time needed to run the loop itself
            time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL*(0.99e-3))
        if not self.cancel_election:
            self.hold_election()
        
    def leader_check(self):
        """
        Checks if the current node is still the leader by decrementing the leader timeout.
        If the leader timeout reaches zero, the node changes its role to Candidate.
        """
        self.leader_timeout = randrange(start=State.LEADER_TIMEOUT_INTERVAL_MIN,
                                        stop=State.LEADER_TIMEOUT_INTERVAL_MAX,
                                        step=State.LEADER_TIMEOUT_POLLING_INTERVAL)
        while self.leader_timeout > 0:
            self.leader_timeout -= State.LEADER_TIMEOUT_POLLING_INTERVAL
            # sleeping for slightly less than a millisecond to account for the time needed to run the loop itself
            time.sleep(State.LEADER_TIMEOUT_POLLING_INTERVAL*(0.99e-3))
        
        self.change_role(State.Candidate)
    
    def periodic_heartbeat(self):
        """
        Sends periodic heartbeats to all nodes in the cluster.

        This method is called when the current role is Leader. It iterates over all nodes in the cluster,
        excluding the current node, and sends a heartbeat message to each node by calling the `replicate_log` method.
        After sending the heartbeats, it sleeps for a specified interval before sending the next set of heartbeats.

        Note: The interval between heartbeats is defined by the `HEARTBEAT_INTERVAL` constant in the `State` class.

        Returns:
            None
        """
        while self._current_role == State.Leader:
            for node in self.node_ids_excluding_self:
                self.replicate_log(node)
            time.sleep((State.HEARTBEAT_INTERVAL/1000))