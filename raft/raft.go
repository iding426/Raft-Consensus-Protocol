package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"
	// "fmt"
	"cs350/labgob"
	"cs350/labrpc"
	"math/rand"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // This peer's index into peers[]
	dead      int32               // Set by Kill()

	// Your data here (4A, 4B).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Tracker for the current term
	currentTerm int

	// Checks who this server voted for in this term, index in peers
	votedFor int

	// List of logs
	logs []Log

	// Current timeout counter
	timeout int

	// State
	state string

	// Voltile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of the highest log entry applied to state machine
	lastLogIndex int // Index of the last log entry

	// Volatile state on leaders
	nextIndex []int // Index of next log entry to send to each server
	matchIndex []int // Index of last matching log for each server

	stateMachine chan ApplyMsg
}

// Log struct
type Log struct {
	Command interface{}
	Term int
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()

	var term int
	var isleader bool

	// Your code here (4A).

	// Get the term and leader status
	term = rf.currentTerm
	if rf.state == "Leader" {
		isleader = true
	}

	// // fmt.Printf("%d is the %s for term: %d \n", rf.me, rf.state, rf.currentTerm)

	rf.mu.Unlock()

	return term, isleader
}

// Save Raft's persistent state to stable storage, where it
// can later be retrieved after a crash and restart. See paper's
// Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (4B).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	currentTerm := 0
	votedFor := 0

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&rf.logs) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = len(rf.logs) - 1
	}

}

// Append Entries RPC arguments structure
type AppendEntriesArgs struct {
	Term int // Leaders term
	LeaderID int // ID of the leader
	PrevLogIndex int // Index of the log entry immediately preceding new ones
	PrevLogTerm int // Term of the last entry in the log
	Entries []Log // List of entries to append, empty for heartbeat 
	LeaderCommmit int // Leader's commit index 
}

// Append Entries RPC reply structure
type AppendEntriesReply struct {
	Term int // currentTerm
	Success bool // Success or fail
}

// RPC Handler for Append Entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.timeout = 0 // Reset the timeout

	reply.Success = false
	reply.Term = rf.currentTerm

	// Reciever Implementation Step 1 
	if args.Term < rf.currentTerm {
		return
	}

	// Terms don't match
	if args.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
		return
	}

	// Reciever Implementation Step 2
	if len(rf.logs) < args.PrevLogIndex + 1 {
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// Reciever Implementation Step 3
	i := 0
	for ; i < len(args.Entries); i++ {
		logIndex := args.PrevLogIndex + i + 1 // + 1 because 0 index of entries should not be lastLogIndex

		// If longer than the follower log then break
		if logIndex > len(rf.logs) - 1 {
			break;
		}

		if rf.logs[logIndex].Term != args.Entries[i].Term {
			rf.logs = rf.logs[:logIndex] // Delete
			rf.lastLogIndex = len(rf.logs) - 1

			break; 
		}
	}

	reply.Success = true

	// Reciever Implementation step 4
	if len(args.Entries) > 0 {
		rf.logs = append(rf.logs, args.Entries[i:]...)
		rf.lastLogIndex = len(rf.logs) - 1

		// fmt.Printf("Server: %d with term %d, recieved AE from leader: %d with term %d, and appends %d entries at: %d \n", rf.me, rf.currentTerm, args.LeaderID, args.Term, len(args.Entries), rf.lastLogIndex)
		rf.persist()
	}

	// Reciever Implementation step 5
	if args.LeaderCommmit > rf.commitIndex {
		if args.LeaderCommmit < rf.lastLogIndex {
			rf.commitIndex = args.LeaderCommmit
		} else {
			rf.commitIndex = rf.lastLogIndex
		}

		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			// Commit i
			rf.stateMachine <- ApplyMsg{
				CommandValid: true,
				CommandIndex: i,
				Command: rf.logs[i].Command,
			}

			// fmt.Printf("Server: %d, Committing Log at Index %d, with term: %d. LEADER COMMIT INDEX OF: %d \n", rf.me, i, rf.logs[i].Term, args.LeaderCommmit)
			rf.lastApplied = i 
		}
	}
}

// Wrapper Function
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (4A, 4B).
	Term int // The candidates term
	CandidateID int // Index of the Candidate trying to get a vote
	LastLogIntex int // Index of the Candidate's LastLogEntry
	LastLogTerm int // Term of the Candidate's LastLogEntry
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (4A).
	Term int // Current Term of the server giving the vote
	VoteGranted bool // True if the vote is granted
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	rf.timeout = 0 // Reset the election timeout 

	// Reciever Implementation Step 1 
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Step down logic 
	if args.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// Reciever Implementation Step 2 
	flag := true
	lastLogTerm := rf.logs[rf.lastLogIndex].Term

	if args.LastLogTerm == lastLogTerm {
		flag = (args.LastLogIntex >= rf.lastLogIndex)
	} else {
		flag = (args.LastLogTerm > lastLogTerm)
	}

	// Grant the vote only if we havent voted and the conitions are met
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && flag {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true;
		rf.votedFor = args.CandidateID

		return
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

}


// Example code to send a RequestVote RPC to a server.
// Server is the index of the target server in rf.peers[].
// Expects RPC arguments in args. Fills in *reply with RPC reply,
// so caller should pass &reply.
//
// The types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// Look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (4B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = (rf.state == "Leader")
	
	// Don't do anything if we arn't leader
	if !isLeader {
		return 0, 0, false
	}

	term = rf.currentTerm // Current term

	// Append entry if we are the leader
	rf.logs = append(rf.logs, Log{Command: command, Term: term})

	rf.lastLogIndex = len(rf.logs) - 1 // Index of the log we just added
	index = rf.lastLogIndex

	rf.persist()

	
	// fmt.Printf("New Log for LEADER: %d at index %d and term %d \n", rf.me, rf.lastLogIndex, rf.currentTerm)

	return index, term, isLeader
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()

		currState := rf.state

		rf.mu.Unlock()

		if currState == "Leader" {

			// Send a heartbeat to each peer
			go func() {

				// Get all metadata we might need
				rf.mu.Lock()
				me := rf.me
				term := rf.currentTerm
				peers := rf.peers

				rf.nextIndex[me] = rf.lastLogIndex + 1
				rf.matchIndex[me] = rf.lastLogIndex
				nextIndex := rf.nextIndex
				matchIndex := rf.matchIndex

				commitIndex := rf.commitIndex
				lastLogIndex := rf.lastLogIndex

				logs := rf.logs

				// Commit Agreed Entries
				
				for n := lastLogIndex; n > commitIndex; n-- {
					count := 0
					for _, value := range matchIndex{
						if value >= n {
							count++
						}
					}

					if count > len(peers) / 2 && logs[n].Term == term{
						rf.commitIndex = n
						break
					}
				}

				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					// Commit i

					// fmt.Printf("Server: %d, Committing Log at Index %d, with term: %d \n", rf.me, i, rf.logs[i].Term)
					// fmt.Println(rf.matchIndex)
					rf.stateMachine <- ApplyMsg{
						CommandValid: true,
						CommandIndex: i,
						Command: logs[i].Command,
					}

					rf.lastApplied = i

				}

				rf.mu.Unlock()

				for peer := range peers {
					if peer == me {
						continue
					}

					// Build args and reply structs 
					rf.mu.Lock()
					prevLogIndex := nextIndex[peer] - 1
					lastTerm := rf.logs[prevLogIndex].Term

					args := AppendEntriesArgs{Term: term, LeaderID: me, PrevLogIndex: prevLogIndex, PrevLogTerm: lastTerm, LeaderCommmit: commitIndex}

					if nextIndex[peer] <= lastLogIndex {
						args.Entries = logs[prevLogIndex + 1: lastLogIndex + 1]
					}

					reply := AppendEntriesReply{}
					rf.mu.Unlock()

					// Send the heartbeat
					go func (peer int) {
						returned := rf.sendAppendEntries(peer, &args, &reply)

						// If RPC failed just skip remaining logic
						if !returned {
							return
						}

						rf.mu.Lock()
						defer rf.mu.Unlock()
						defer rf.persist()

						// Step down
						if reply.Term > rf.currentTerm {
							rf.state = "Follower"
							rf.currentTerm = reply.Term
							rf.votedFor = -1

							return
						}

						if rf.state != "Leader" || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
							return
						}

						// Made it to the point that append entries checked log consistency
						if reply.Success {
							// Successfully appended
							newMatchIndex := args.PrevLogIndex + len(args.Entries)
							if newMatchIndex > rf.matchIndex[peer] {
								rf.matchIndex[peer] = newMatchIndex
							}
							
							rf.nextIndex[peer] = rf.matchIndex[peer] + 1
						} else {
							// Failed due to inconsistent logs 
							rf.nextIndex[peer]--
							rf.matchIndex[peer] = rf.nextIndex[peer] - 1
						}

					}(peer)
				}

			}()

		} else {
			electionTimeout := rand.Intn(250) + 250

			rf.mu.Lock()
			timeoutCounter := rf.timeout
			rf.mu.Unlock()

			if electionTimeout < timeoutCounter {
				go func() {
					// Timeout, start an election
					rf.mu.Lock()
					// fmt.Printf("Server %d timed out for term %d \n", rf.me, rf.currentTerm)
					rf.timeout = 0 // Reset timeout

					// Change state, term, and votedFor
					rf.state = "Candidate"
					rf.currentTerm++
					rf.votedFor = rf.me

					// Grab metadata we may need
					peers := rf.peers
					term := rf.currentTerm
					me := rf.me
					lastLogIndex := rf.lastLogIndex
					lastLogTerm := 0

					if len(rf.logs) > 0 {
						lastLogTerm = rf.logs[lastLogIndex].Term
					}
					rf.mu.Unlock()

					var wait sync.WaitGroup
					voteCount := 1

					for peer := range peers {
						if peer == me {
							continue
						}

						wait.Add(1)

						go func (peer int) {
							args := RequestVoteArgs{Term: term, CandidateID: me, LastLogIntex:  lastLogIndex, LastLogTerm: lastLogTerm}
							reply := RequestVoteReply{Term: -1, VoteGranted: false}

							returned := rf.sendRequestVote(peer, &args, &reply)

							rf.mu.Lock()
							defer rf.mu.Unlock()
							// Step down
							if reply.Term > term {
								rf.currentTerm = reply.Term
								rf.state = "Follower"
								rf.votedFor = -1
								rf.timeout = 0
								rf.persist()
							}

							// Check vote result
							if returned {
								if reply.VoteGranted {
									voteCount += 1
								}
							}

							wait.Done()
							
						}(peer)
					}

					wait.Wait() // Wait for RPCs

					rf.mu.Lock()
					if rf.state == "Candidate" {
						// Didn't step down during election

						if voteCount > len(rf.peers) / 2 {
							rf.state = "Leader"
							// fmt.Printf("Elected Server: %d as leader for term: %d \n", rf.me, rf.currentTerm)
						} else {
							rf.timeout = 0
						}
					}

					rf.mu.Unlock()

				}()
			}
		}

		time.Sleep(time.Millisecond * time.Duration(100))

		rf.mu.Lock()
		rf.timeout += 100
		rf.mu.Unlock()

	}
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (4A, 4B).
	rf.currentTerm = 0
	rf.votedFor = -1 // -1 to represent null
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timeout = 0 
	rf.lastLogIndex = 0
	rf.state = "Follower"

	rf.logs = []Log{
		{
			Command: nil,
			Term: 0,
		},
	}
	
	rf.stateMachine = applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash.
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections.
	go rf.ticker()

	return rf
}
