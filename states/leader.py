from .state import *

class Leader(State):
    HEARTBEAT_INTERVAL = 500

    def __init__(self, server, current_term, voted, log, commit_length):
        super(Leader, self).__init__(server, current_term, voted, log, commit_length)

        self.current_role = State.Leader
        self.sent_lengths: list[int] = [len(self.log) for _ in range(len(self.node_ids))]
        self.acked_lengths: list[int] = [0 for _ in range(len(self.node_ids))]

    def create_and_start_threads(self):
        self.running_thread = StoppableThread(target=self.periodic_heartbeat, args=tuple())
        self.running_thread.start()

    def on_client_request(self, message: ClientRequest):
        # Add the received instruction to the log and update the acknowledged length for the leader itself.
        instruction = message.instruction_to_add
        self.log.append((self.current_term, instruction))
        self.acked_lengths[self.id] = len(self.log)
        # Replicate the log to all follower nodes
        for follower_node in self.node_ids_excluding_self:
            self.replicate_log(follower_node)
    
    def periodic_heartbeat(self):
        while not self.running_thread.stopped():
            for node in self.node_ids_excluding_self:  # Iterate over all nodes in the cluster, excluding the current node
                self.replicate_log(node)  # Send a heartbeat message to each node by calling the `replicate_log` method
            time.sleep((Leader.HEARTBEAT_INTERVAL/1000))  # Sleep for a specified interval before sending the next set of heartbeats

    # exclusive to leader
    def replicate_log(self, follower_id):
        # Get the length of the log that has already been sent to the follower
        prefix_len = self.sent_lengths[follower_id]
    
        # Extract the remaining log entries that need to be sent to the follower
        suffix = self.log[prefix_len:-1]
    
        # Determine the term of the log entry just before the prefix length
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len - 1][0]
    
        # Create a message to send to the follower containing relevant log information
        print("send a log record to the follower", self.current_role, self.id)
        msg_to_send = LogRequest(self.id, self.current_term, prefix_len, prefix_term, self.commit_length, suffix)
    
        # Send the message to the follower
        self.send_message(follower_id, msg_to_send)

    # only leader responds
    def on_log_response(self, message: LogResponse):
        follower_id = message.follower_id
        term = message.term
        ack = message.ack
        success = message.success

        # First, we need to check that we are not behind the server that has responded to us.
        # If the server sending this message is behind, we just ignore it; it will be updated
        # by the leader sometime in the future.
        # Assuming we were not behind, we then also need to check we are still a leader; because
        # by the time this response has come back to us, things might have happened that made us revert back to a follower.
        if term == self.current_term:
            # ack is checked against _acked_length registered by the leader just in case a log_request was received
            # by a follower, and also committed successfully, but the response failed to reach the leader 
            # in a timely manner (for whatever reason). In this case, even if the follower send a newer log response
            # that reached before the older log response, we won't waste time on commit_log_entries.
            if success == True and ack >= self.acked_lengths[follower_id]:
                # Update the sent length and acknowledged length for the follower
                self.sent_lengths[follower_id] = ack
                self.acked_lengths[follower_id] = ack
                self.commit_log_entries()
            # if the response declares failure, the follower either had even smaller of a prefix than we originally thought,
            # or it had an inconsistent log at the index 'prefix_len'. So we need to keep going back and sending logs farther
            # back until we reach a prefix that is consistent.
            elif self.sent_lengths[follower_id] > 0:
                self.sent_lengths[follower_id] -= 1
                self.replicate_log(follower_id)
        # If we are actually behind, then we need to give up leadership and become a follower.
        # One more note: if we actually demote this way, then we won't have the correct leader for a while,
        # but the moment the next heartbeat from a legitimate leader comes through, we'll get the leader's id.
        elif term > self.current_term:
            print("KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK\n", message, "\nKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKkk\n")
            self.current_term = term
            self.change_role(State.Follower)
        write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})
    