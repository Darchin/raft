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
        record = message._record  # Get the record from the message object
        # Add the received record to the log and update the acknowledged length for the current node
        if self._current_role == State.Leader:
            self._log.append((self._current_term, record))
            self._acked_length[self.id] = len(self._log)
            # Replicate the log to all follower nodes
            for follower_node in self.node_ids_excluding_self:
                self.replicate_log(follower_node)
        else:
            # Forward client request to the leader if received by a follower
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

        # Check if the received term is greater than the current term
        if term > self._current_term:
            self._current_term = term
            self._voted_for = set([None])
            self.cancel_election = True

        # Check if the received term is equal to the current term
        if term == self._current_term:
            self.change_role(State.Follower)
            self._current_leader = leader_id

        # Check if the log is consistent with the received prefix length and term
        log_ok = (len(self._log) >= prefix_len) and (prefix_len == 0 or self._log[prefix_len - 1] == prefix_term)

        # If the term and log are consistent, append the entries to the log
        if term == self._current_term and log_ok:
            self.append_entries(prefix_len, leader_commit, suffix)
            ack = prefix_len + len(suffix)
            msg_to_send = Message(Message.LogResponse, node_id=self.server._id, current_term=self._current_term,
                                  ack=ack, success=True)
        else:
            # If the term or log are not consistent, send a failure response
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

        # Check if the term matches the current term and if the current role is Leader
        if term == self._current_term and self._current_role == State.Leader:
            # Check if the log response was successful and if the acknowledgement is greater than or equal to the acknowledged length for the follower
            if success == True and ack >= self._acked_length[follower_id]:
                # Update the sent length and acknowledged length for the follower
                self._sent_length[follower_id] = ack
                self._acked_length[follower_id] = ack
                # Commit the log entries
                self.commit_log_entries()
            # If the sent length for the follower is greater than 0
            elif self._sent_length[follower_id] > 0:
                # Decrement the sent length for the follower
                self._sent_length[follower_id] -= 1
                # Replicate the log for the follower
                self.replicate_log(follower_id)
        # If the term is greater than the current term
        elif term > self._current_term:
            # Update the current term and current role to Follower
            self._current_term = term
            # Reset the voted_for set to include None
            self._voted_for = set([None])
            # Cancel the ongoing election
            self.cancel_election = True
            # Sleep to allow election phase change
            self.change_role(State.Follower)
    
    def on_vote_request(self, message):
        """
        Handles a vote request from a candidate.

        Args:
            message: The vote request message containing candidate information.

        Returns:
            None
        """
        # Extract candidate information from the message
        candidate_id = message.candidate_id
        candidate_term = message.candidate_term
        candidate_log_length = message.candidate_log_length
        candidate_log_term = message.candidate_log_term
        vote_granted = False

        # Check if the candidate's term is greater than the current term
        if candidate_term > self._current_term:
            # Update the current term to the candidate's term
            self._current_term = candidate_term
            # Change the role to Follower
            self.change_role(State.Follower)
            # Reset the voted_for set to allow voting for a new candidate
            self._voted_for = set([None])

        # Get the last term in the log, or 0 if the log is empty
        last_term = self._log[-1][0] if len(self._log) > 0 else 0

        # Check if the candidate's log is up-to-date
        log_ok = (candidate_log_term > last_term) or (candidate_log_term == last_term and candidate_log_length >= len(self._log))

        # Check if the candidate is eligible for vote
        if candidate_term == self._current_term and log_ok and self._voted_for.issubset(set([None, candidate_id])):
            # Update the voted_for set to the candidate_id
            self._voted_for = set([candidate_id])
            # Grant the vote
            vote_granted = True

        # Create a VoteResponse message with the current node's information
        msg_to_send = VoteResponse(self.id, self._current_term, vote_granted)
        # Send the VoteResponse message to the candidate
        self.send_message(candidate_id, msg_to_send)

    def on_vote_response(self, message):
        """
        Process the vote response received from a voter.

        Args:
            message: The vote response message containing voter_id, term, and granted.

        Returns:
            None
        """
        # Extract voter_id, term, and granted from the message
        voter_id = message.voter_id
        term = message.term
        granted = message.granted

        # Check if the current role is Candidate and the received term matches the current term, and the vote is granted
        if self._current_role == State.Candidate and term == self._current_term and granted:
            # Add the voter_id to the set of received votes
            self._votes_received.union(set([voter_id]))
            
            # Check if the number of received votes is greater than or equal to the majority required
            if len(self._votes_received) >= self.node_majority:
                # Set the cancel_election flag to True
                self.cancel_election = True
                
                # Give enough time for the election cancellation status polling to register the change
                time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL/1000)
                
                # Change the role to Leader
                self.change_role(State.Leader)
                
        # If the received term is greater than the current term
        elif term > self._current_term:
            # Set the cancel_election flag to True
            self.cancel_election = True
            
            # Give enough time for the election cancellation status polling to register the change
            time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL/1000)
            
            # Update the current term to the received term
            self._current_term = term
            
            # Change the role to Follower
            self.change_role(State.Follower)
            
            # Reset the voted_for set to None
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
        
        # Check if there are entries to append and if the prefix length is valid
        if suffix_len > 0 and len(self._log) > prefix_len:
            # Calculate the index of the last entry to compare with the suffix
            index = min(len(self._log), prefix_len + suffix_len) - 1
            
            # Compare the last entry in the log with the corresponding entry in the suffix
            if self._log[index][0] != suffix[index-prefix_len][0]:
                # If the entries don't match, remove all entries after the prefix
                self._log = self._log[:prefix_len]

        # Check if the combined length of the prefix and suffix is greater than the current log length
        if prefix_len + suffix_len > len(self._log):
            # Append the remaining entries in the suffix to the log
            for i in range(len(self._log) - prefix_len, suffix_len):
                self._log.append(suffix[i])

        # Check if the leader's commit length is greater than the current commit length
        if leader_commit > self._commit_length:
            # Deliver the entries to the server starting from the current commit length
            for i in range(self._commit_length, leader_commit):
                self.deliver_to_server(self._log[i][1])
            
            # Update the commit length to match the leader's commit length
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
        # Get the length of the log that has already been sent to the follower
        prefix_len = self._sent_length[follower_id]
    
        # Extract the remaining log entries that need to be sent to the follower
        suffix = self._log[prefix_len:-1]
    
        # Determine the term of the log entry just before the prefix length
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self._log[prefix_len - 1][0]
    
        # Create a message to send to the follower containing relevant log information
        msg_to_send = LogRequest(self.id, self._current_term, prefix_len, prefix_term, self._commit_length, suffix)
    
        # Send the message to the follower
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
            # This nested function, acks, calculates the number of nodes that have acknowledged a log entry of a given length.
            ack_nodes = set()
            for node in self.node_ids:
                if self._acked_length[node] >= length:
                    ack_nodes.add(node)
            return len(ack_nodes)

        min_acks = self.node_majority
        # The minimum number of acknowledgments required for a log entry to be considered acknowledged by a majority of nodes.

        ready = set([l for l in range(1, len(self._log)+1) if acks(l) >= min_acks])
        # The ready set contains the indices of log entries that have received acknowledgments from a majority of nodes.
        # It uses a list comprehension to iterate over the range of log entry indices and checks if the number of acknowledgments
        # for each log entry is greater than or equal to the minimum required acknowledgments.

        highest_rdy_to_commit = max(ready)
        # The highest_rdy_to_commit variable stores the index of the highest log entry that is ready to be committed.

        if len(ready) != 0 and highest_rdy_to_commit > self._commit_length and self._log[highest_rdy_to_commit - 1][0] == self._current_term:
            # This condition checks if there are any log entries ready to be committed, if the highest ready log entry index is greater
            # than the current commit length, and if the term of the highest ready log entry matches the current term.

            for i in range(self._commit_length, highest_rdy_to_commit):
                # This loop iterates over the log entries starting from the current commit length up to the highest ready log entry index.
                # It delivers each log entry to the server by calling the deliver_to_server method with the corresponding entry value.

                self.deliver_to_server(self._log[i][1])

            self._commit_length = highest_rdy_to_commit
            # After committing the log entries, the commit length is updated to the index of the highest ready log entry.
    
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

        # Use a match statement to determine the new role
        match new_role:
            case State.Follower:
                # Set the current role to Follower
                prev_role = self._current_role
                self._current_role = State.Follower
                # Start a new thread to check for leader status
                # If node was already a follower, no need to start another thread
                if prev_role == State.Follower:
                    threading.Thread(target=self.leader_check, args=tuple()).start()

            case State.Candidate:
                # Set the current role to Candidate
                self._current_role = State.Candidate
                # Initialize the voted_for and votes_received sets
                self._voted_for = set([self.id])
                self._votes_received = set([self.id])
                # Start a new thread to hold an election
                threading.Thread(target=self.hold_election, args=tuple()).start()

            case State.Leader:
                # Set the current role to Leader
                self._current_role = State.Leader
                # Set the current leader to the node's own ID
                self._current_leader = self.id
                
                # Iterate over each follower node
                for follower_node in self.node_ids_excluding_self:
                    # Set the sent_length for each follower to the length of the log
                    self._sent_length[follower_node] = len(self._log)
                    # Set the acked_length for each follower to 0
                    self._acked_length[follower_node] = 0
                    # Replicate the log to each follower
                    self.replicate_log(follower_node)

                # Start a new thread for periodic heartbeat messages
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
        
        # Set the election timeout to the predefined value
        self.election_timeout = State.CANDIDATE_ELECTION_TIMEOUT
        
        # Increment the current term of the node
        self._current_term += 1
        
        # Determine the last term in the log, if it exists
        last_term = self._log[-1][0] if len(self._log) > 0 else 0
        
        # Prepare a vote request message with the node's ID, current term, log length, and last term
        msg_to_send = VoteRequest(self.id, self._current_term, len(self._log), last_term)
        
        # Send the vote request message to all other nodes except self
        for node in self.node_ids_excluding_self:
            self.send_message(node, msg_to_send)
        
        # Wait for the election timeout to expire or until the election is canceled
        while self.election_timeout > 0 and not self.cancel_election:
            # Decrease the election timeout by the polling interval
            self.election_timeout -= State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL
            
            # Sleep for slightly less than a millisecond to account for the time needed to run the loop itself
            time.sleep(State.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL * (0.99e-3))
        
        # If the election is not canceled, recursively call the hold_election method to hold a new election
        if not self.cancel_election:
            self.hold_election()
        
    def leader_check(self):
        """
        Checks if the current node is still the leader by decrementing the leader timeout.
        If the leader timeout reaches zero, the node changes its role to Candidate.
        """
        # Generate a random leader timeout value within a specified range
        self.leader_timeout = randrange(start=State.LEADER_TIMEOUT_INTERVAL_MIN,
                                        stop=State.LEADER_TIMEOUT_INTERVAL_MAX,
                                        step=State.LEADER_TIMEOUT_POLLING_INTERVAL)
        
        # Loop until the leader timeout reaches zero
        while self.leader_timeout > 0:
            # Decrement the leader timeout by the polling interval
            self.leader_timeout -= State.LEADER_TIMEOUT_POLLING_INTERVAL
            
            # Sleep for slightly less than a millisecond to account for the time needed to run the loop itself
            time.sleep(State.LEADER_TIMEOUT_POLLING_INTERVAL * (0.99e-3))
        
        # Change the role of the node to Candidate
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
        while self._current_role == State.Leader:  # Continuously execute the following code while the current role is Leader
            for node in self.node_ids_excluding_self:  # Iterate over all nodes in the cluster, excluding the current node
                self.replicate_log(node)  # Send a heartbeat message to each node by calling the `replicate_log` method
            time.sleep((State.HEARTBEAT_INTERVAL/1000))  # Sleep for a specified interval before sending the next set of heartbeats