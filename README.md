# RAFT Consensus Protocol 

** Implemented in raft/raft.go **

This project implements the RAFT Consensus protocol outlined in by Diego Ongaro and John Ousterhout of Stanford University in [this paper](https://raft.github.io/raft.pdf). RAFT is a consensus protocol 
that produces a result equivalent to [Paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf), a reliable consensus protocol for managing a replicated log. 
This implementation passes all of the tests required for the MIT course 6.5840 (Graduate Distributed Systems) RAFT lab. The working solution ensures a truely robust, reliable, and fault tolerant 
distributed approach to creating a *replicated state machine* for *primary backup replication* with an outlined overview below. 

![Screenshot_6-8-2024_222710_raft github io](https://github.com/user-attachments/assets/75e99f2e-efdd-4660-8b62-7a7aa4884ac3)

## Table of Contents 
- [Node Rules](#node-rules)
- [Leader Election](#leader-election)
- [Log Replication](#log-replication)

## Node Rules
There are a few rules for all nodes that ensures the safety of all committed logs and proper communication between all nodes. 

- If a higher term node contacts a node, the contacted node will update its term to the most recent term.
- Leaders and Candidates will step down if a node with higher term claims to be a leader
- Node information is saved to disk to allow the system to remain persistent through individual failures allowing them to rejoin the system without wiping their data.

## Leader Election
The protocol has a strict rule of *at most* one leader to avoid divergent logs. Leaders inform other nodes that they are alive by sending heartbeats every few milliseconds. 
A term starts when a new leader is elected.
If a node doesn't recieve a heartbeat from the leader within a certain amount of time, it will start an election and if it gains a strict majority of the votes for the current term, it will become then new leader. 
Each node can only vote once per term, so that means there can never be two leaders for a term. 
Votes are sent on the following conditions
 
 - A node has not voted yet for this term.
 - The candidate has equal or higher term than the voter.
 - The candidates logs is at least as up to date as the voter's (so a node will never vote for a candidate without committed logs) 

The voting and heartbeats for nodes is handled using RPC.

In the case that a node becomes a leader but recieves a heartbeat from another node claiming to be a leader, the node with lower term will step down as it may have stale logs. 

## Log Replication

Logs are forwarded from the leader to the other nodes by as apart of the heartbeats (some heartbeats may be empty). As soon as a majority of the nodes have appended this log to 
their logs, then this log is considered committed.

Committed logs can never be lost, because the user is informed of their completion. This means all leaders must always contain *every* committed log. This is handled by the election rules stated in part
one. 

Upon commit, the leader will inform all of its followers that a node is committed. 

In the case of a divergent log between a leader and follower the follower will delete its previous log and return a failure to the leader, who will retry the request with an earlier log. 
This is repeated until the divergent log finds a convergence point and updates all of its logs following that point. 
