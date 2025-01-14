package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role Role
	term int

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer
	notifyApplyCh       chan struct{}
	stopCh              chan struct{}

	voteFor           int
	logEntries        []LogEntry
	applyCh           chan ApplyMsg
	commitIndex       int
	lastSnapshotIndex int
	lastSnapshotTerm  int
	lastApplied       int
	nextIndex         []int
	matchIndex        []int

	DebugLog  bool
	lockStart time.Time
	lockEnd   time.Time
	lockName  string
	gid       int
}

type LogEntry struct {
	Term    int
	Idx     int
	Command interface{}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

const (
	ElectionTimeout  = time.Millisecond * 300
	HeartBeatTimeout = time.Millisecond * 150
	ApplyInterval    = time.Millisecond * 100
	RPCTimeout       = time.Millisecond * 100
	MaxLockTime      = time.Millisecond * 10
)

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.lock("get state")
	defer rf.unlock("get state")
	return rf.term, rf.role == Leader
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lock("req_vote")
	defer rf.unlock("req_vote")
	defer func() {
		rf.log("get request vote, args:%+v, reply:%+v", args, reply)
	}()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	} else if args.Term == rf.term {
		if rf.role == Leader {
			return
		}
		if rf.voteFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
		if rf.voteFor != -1 && rf.voteFor != args.CandidateId {
			return
		}
	}

	defer rf.persist()
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.changeRole(Follower)
	}

	if lastLogTerm > args.LastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}

	rf.term = args.Term
	rf.voteFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	rf.log("vote for:%d", args.CandidateId)
	return
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randElectionTimeout())
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
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
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	t := time.NewTimer(RPCTimeout)
	defer t.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		r := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &r)
			if ok == false {
				time.Sleep(time.Millisecond * 10)
			}
			ch <- ok
		}()

		select {
		case <-t.C:
			return
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if !ok {
				continue
			} else {
				reply.Term = r.Term
				reply.VoteGranted = r.VoteGranted
				return
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.lock("start_election")
	rf.electionTimer.Reset(randElectionTimeout())
	if rf.role == Leader {
		rf.unlock("start_election")
		return
	}
	rf.log("start election")
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.unlock("start_election")

	grantedCount := 1
	chResCount := 1
	votesCh := make(chan bool, len(rf.peers))
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.lock("start_ele_change_term")
				if rf.term < reply.Term {
					rf.term = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start_ele_change_term")
			}
		}(votesCh, index)
	}

	for {
		r := <-votesCh
		chResCount += 1
		if r == true {
			grantedCount += 1
		}
		if chResCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || chResCount-grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		rf.log("grantedCount <= len/2:count:%d", grantedCount)
		return
	}

	rf.lock("start_ele2")
	rf.log("before try change to leader,count:%d, args:%+v", grantedCount, args)
	if rf.term == args.Term && rf.role == Candidate {
		rf.changeRole(Leader)
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetHeartBeatTimers()
	}
	rf.unlock("start_ele2")
}

func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) outOfOrderAppendEntries(args *AppendEntriesArgs) bool {
	argsLastIndex := args.PrevLogIndex + len(args.Entries)
	lastTerm, lastIndex := rf.lastLogTermIndex()
	if argsLastIndex < lastIndex && lastTerm == args.Term {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("append_entries")
	rf.log("get appendentries:%+v", *args)
	reply.Term = rf.term

	if rf.term > args.Term {
		rf.unlock("append_entries")
		return
	}

	rf.term = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()
	_, lastLogIndex := rf.lastLogTermIndex()

	if args.PrevLogIndex < rf.lastSnapshotIndex {
		reply.Success = false
		reply.NextIndex = rf.lastSnapshotIndex + 1
	} else if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if args.PrevLogIndex == rf.lastSnapshotIndex {
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[:1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	} else if rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term == args.PervLogTerm {
		if rf.outOfOrderAppendEntries(args) {
			reply.Success = false
			reply.NextIndex = 0
		} else {
			reply.Success = true
			rf.logEntries = append(rf.logEntries[0:rf.getRealIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			reply.NextIndex = rf.getNextIndex()
		}
	} else {
		rf.log("prev log not match")
		reply.Success = false
		term := rf.logEntries[rf.getRealIdxByLogIndex(args.PrevLogIndex)].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && idx > rf.lastSnapshotIndex && rf.logEntries[rf.getRealIdxByLogIndex(idx)].Term == term {
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}
	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.persist()
	rf.log("get appendentries:%+v, reply:%+v", *args, *reply)
	rf.unlock("append_entries")
}

func (rf *Raft) getAppendLogs(peerIdx int) (prevLogIndex, prevLogTerm int, res []LogEntry) {
	nextIdx := rf.nextIndex[peerIdx]
	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	if nextIdx <= rf.lastSnapshotIndex || nextIdx > lastLogIndex {
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
		return
	}

	res = append([]LogEntry{}, rf.logEntries[rf.getRealIdxByLogIndex(nextIdx):]...)
	prevLogIndex = nextIdx - 1
	if prevLogIndex == rf.lastSnapshotIndex {
		prevLogTerm = rf.lastSnapshotTerm
	} else {
		prevLogTerm = rf.getLogByIndex(prevLogIndex).Term
	}
	return
}

func (rf *Raft) getAppendEntriesArgs(peerIdx int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm, logs := rf.getAppendLogs(peerIdx)
	args := AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PervLogTerm:  prevLogTerm,
		Entries:      logs,
		LeaderCommit: rf.commitIndex,
	}
	return args
}

func (rf *Raft) resetHeartBeatTimers() {
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartBeatTimer(peerIdx int) {
	rf.appendEntriesTimers[peerIdx].Stop()
	rf.appendEntriesTimers[peerIdx].Reset(HeartBeatTimeout)
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()

	for !rf.killed() {
		rf.lock("appendtopeer1")
		if rf.role != Leader {
			rf.resetHeartBeatTimer(peerIdx)
			rf.unlock("appendtopeer1")
			return
		}
		args := rf.getAppendEntriesArgs(peerIdx)
		rf.resetHeartBeatTimer(peerIdx)
		rf.unlock("appendtopeer1")

		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(time.Millisecond * 10)
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <-rf.stopCh:
			return
		case <-RPCTimer.C:
			rf.log("appendtopeer, rpctimeout: peer:%d, args:%+v", peerIdx, args)
			continue
		case ok := <-resCh:
			if !ok {
				rf.log("appendtopeer not ok")
				continue
			}
		}

		rf.log("appendtoperr, peer:%d, args:%+v, reply:%+v", peerIdx, args, reply)
		rf.lock("appendtopeer2")
		if reply.Term > rf.term {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.term = reply.Term
			rf.persist()
			rf.unlock("appendtopeer2")
			return
		}

		if rf.role != Leader || rf.term != args.Term {
			rf.unlock("appendtopeer2")
			return
		}

		if reply.Success {
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.term {
				rf.updateCommitIndex()
			}
			rf.persist()
			rf.unlock("appendtopeer2")
			return
		}

		if reply.NextIndex != 0 {
			if reply.NextIndex > rf.lastSnapshotIndex {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.unlock("appendtopeer2")
				continue
			} else {
				rf.unlock("appendtopeer2")
				return
			}
		} else {
			rf.unlock("appendtopeer2")
		}
	}

}

func (rf *Raft) updateCommitIndex() {
	rf.log("in update commitindex")
	hasCommit := false
	for i := rf.commitIndex + 1; i <= rf.lastSnapshotIndex+len(rf.logEntries); i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					rf.log("update commit index:%d", i)
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	// data := rf.getPersistData()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.voteFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.Save(data, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var voteFor int
	var logs []LogEntry
	var commitIndex, lastSnapshotIndex, lastSnapshotTerm int

	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("rf read persist err")
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.commitIndex = commitIndex
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		rf.logEntries = logs
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

func (rf *Raft) lock(m string) {
	rf.mu.Lock()
	rf.lockStart = time.Now()
	rf.lockName = m
}

func (rf *Raft) unlock(m string) {
	rf.lockEnd = time.Now()
	rf.lockName = ""
	duration := rf.lockEnd.Sub(rf.lockStart)
	if rf.lockName != "" && duration > MaxLockTime {
		rf.log("lock too long:%s:%s:iskill:%v", m, duration, rf.killed())
	}
	rf.mu.Unlock()
}

func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Follower:
	case Candidate:
		rf.term += 1
		rf.voteFor = rf.me
		rf.resetElectionTimer()
	case Leader:
		_, lastLogIndex := rf.lastLogTermIndex()
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastLogIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()
	default:
		panic("unknown role")
	}

}

func (rf *Raft) lastLogTermIndex() (int, int) {
	term := rf.logEntries[len(rf.logEntries)-1].Term
	index := rf.lastSnapshotIndex + len(rf.logEntries) - 1
	return term, index
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.lock("start")
	term := rf.term
	isLeader := rf.role == Leader
	_, lastIndex := rf.lastLogTermIndex()
	index := lastIndex + 1

	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.term,
			Command: command,
			Idx:     index,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	rf.resetHeartBeatTimers()
	rf.unlock("start")
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) log(format string, a ...interface{}) {
	if rf.DebugLog == false {
		return
	}
	term, idx := rf.lastLogTermIndex()
	r := fmt.Sprintf(format, a...)
	s := fmt.Sprintf("gid:%d, me: %d, role:%v,term:%d, commitIdx: %v, snidx:%d, apply:%v, matchidx: %v, nextidx:%+v, lastlogterm:%d,idx:%d",
		rf.gid, rf.me, rf.role, rf.term, rf.commitIndex, rf.lastSnapshotIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, term, idx)
	log.Printf("%s:log:%s\n", s, r)
}

func (rf *Raft) startApplyLogs() {
	defer rf.applyTimer.Reset(ApplyInterval)

	rf.lock("applyLogs1")
	var msgs []ApplyMsg
	if rf.lastApplied < rf.lastSnapshotIndex {
		msgs = make([]ApplyMsg, 0, 1)
		msgs = append(msgs, ApplyMsg{
			CommandValid: false,
			Command:      "installSnapShot",
			CommandIndex: rf.lastSnapshotIndex,
		})

	} else if rf.commitIndex <= rf.lastApplied {
		msgs = make([]ApplyMsg, 0)
	} else {
		rf.log("rfapply")
		msgs = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.getRealIdxByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.unlock("applyLogs1")

	for _, msg := range msgs {
		rf.applyCh <- msg
		rf.lock("applyLogs2")
		rf.log("send applych idx:%d", msg.CommandIndex)
		rf.lastApplied = msg.CommandIndex
		rf.unlock("applyLogs2")
	}
}

func (rf *Raft) getLogByIndex(logIndex int) LogEntry {
	idx := logIndex - rf.lastSnapshotIndex
	return rf.logEntries[idx]
}

func (rf *Raft) getRealIdxByLogIndex(logIndex int) int {
	idx := logIndex - rf.lastSnapshotIndex
	if idx < 0 {
		return -1
	} else {
		return idx
	}
}

func randElectionTimeout() time.Duration {
	r := time.Duration(rand.Int63()) % ElectionTimeout
	return ElectionTimeout + r
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.DebugLog = false

	rf.stopCh = make(chan struct{})
	rf.term = 0
	rf.voteFor = -1
	rf.role = Follower
	rf.logEntries = make([]LogEntry, 1)
	rf.readPersist(persister.ReadRaftState())

	// Your initialization code here (3A, 3B, 3C).

	rf.electionTimer = time.NewTimer(randElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, len(rf.peers))
	for i, _ := range rf.peers {
		rf.appendEntriesTimers[i] = time.NewTimer(HeartBeatTimeout)
	}
	rf.applyTimer = time.NewTimer(ApplyInterval)
	rf.notifyApplyCh = make(chan struct{}, 100)

	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.applyTimer.C:
				rf.notifyApplyCh <- struct{}{}
			case <-rf.notifyApplyCh:
				rf.startApplyLogs()
			}
		}
	}()

	go func() {
		for {
			select {
			case <-rf.stopCh:
				return
			case <-rf.electionTimer.C:
				rf.startElection()
			}
		}
	}()

	for i, _ := range peers {
		if i == rf.me {
			continue
		}
		go func(index int) {
			for {
				select {
				case <-rf.stopCh:
					return
				case <-rf.appendEntriesTimers[index].C:
					rf.appendEntriesToPeer(index)
				}
			}
		}(i)
	}

	return rf
}
