import threading

from states.follower import Follower
from .state import *

class Candidate(Follower):
    CANDIDATE_ELECTION_TIMEOUT = 3000
    CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL = 50
    

    def  __init__(self, server, current_term, voted, log, commit_length):
        super(Candidate, self).__init__(server, current_term, voted, log, commit_length)
        self.current_role = State.Candidate
        self.start_election = False
        self.votes_received = set([self.id])

    def create_and_start_threads(self):
        if self.start_election == False:
            self.running_thread = StoppableThread(target=self.hold_election, args=tuple())
            self.running_thread.start()

    def hold_election(self):
        print("[Election]: Starting the election...")
        election_count = 1
        self.start_election = True
        while not self.running_thread.stopped():
            print(threading.current_thread().ident)
            if election_count > 1:
                print("[Election]: Election timed out, restarting election.")
            print(f"[Election]: Currently on election #{election_count} in term {self.current_term}.")
            self.remaining_election_timeout = Candidate.CANDIDATE_ELECTION_TIMEOUT
            
            self.current_term += 1
            write_to_json(f'./stored_data/{self.id}.json', {'current_term':self.current_term, 'voted':self.voted, 'log':self.log, 'commit_length':self.commit_length})
            last_term = self.log[-1][0] if len(self.log) > 0 else 0
            
            # Prepare a vote request message with the node's ID, current term, log length, and last term
            msg_to_send = VoteRequest(self.id, self.current_term, len(self.log), last_term)
            
            # Send the vote request message to all other nodes except self
            for node in self.node_ids_excluding_self:
                self.send_message(node, msg_to_send)
            
            # Wait for the election timeout to expire or until the election is canceled
            while self.remaining_election_timeout > 0 and not self.running_thread.stopped():
                self.remaining_election_timeout -= Candidate.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL
                time.sleep(Candidate.CANDIDATE_ELECTION_TIMEOUT_POLLING_INTERVAL * (1e-3))
            election_count += 1
            
        # only candidate responds
    def on_vote_response(self, message: VoteResponse):
        # Extract voter_id, term, and granted from the message
        voter_id = message.voter_id
        term = message.current_term
        granted = message.granted

        # First we need to check if we are still holding an election (ie we are a candidate),
        # because we might have cancelled our election for whatever reason before this response got to us.
        # If we are still a candidate, and the follower did give us a vote, this can mean two things:
        # 1: our log was outdated.
        # 2: they had already voted for someone else.
        # If we have a bad log, then naturally we won't be getting votes from nodes and just lose the election
        # over and over again until someone else wins the election and becomes a leader. Then they will
        # end our election and update our log to fix/remove the inconsistencies.
        if term == self.current_term and granted:
            # Add the voter_id to the set of received votes
            self.votes_received = self.votes_received.union(set([voter_id]))
            print(f"[Election]: ################ Received votes so far: {self.votes_received}")
            
            # If after receiving this vote, we have gotten majority votes, then we can become a leader.
            if len(self.votes_received) >= self.node_majority:
                self.start_election = False
                self.change_role(State.Leader)
        # If our vote request was rejected, we can still
        # use the term information from the sender. If they are more up-to-date compared to us,
        # we'll use their term to catch ourselves up to speed, and revert to follower in case we already weren't.
        elif term > self.current_term:
            self.current_term = term
            self.start_election = False
            self.change_role(State.Follower)