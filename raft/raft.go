package raft

import (

	"time"
	"sync"
	"math/rand"
	"strconv"


	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)
type Role int
const(
	Follower Role = iota
	Candidate
	Leader
) 

// Raft instance
type Raft struct {
	paxi.Node

	currentTerm int
	votedFor paxi.ID
	log []LogEntry
	commitIndex int
	lastApplied int
	nextIndex sync.Map //map[paxi.ID]int
	matchIndex sync.Map
	logLength int

	role Role
	electionResetEvent time.Time
	quorum *paxi.Quorum
	requestList sync.Map //map[int]*paxi.Request
	mu sync.Mutex

	crash bool

}

var config paxi.Config
var leaderID paxi.ID


func NewRaft(n paxi.Node) *Raft {
	config = paxi.GetConfig()
	ids:=config.IDs()
	raft := &Raft{
		Node:n,
		currentTerm:0,
		votedFor:"-1",
		commitIndex:-1,
		lastApplied:-1,
		role:Follower,
		crash:false,
		quorum:paxi.NewQuorum(),
	}
	for _,peerId := range ids{
		raft.matchIndex.Store(peerId,-1)
	}
	return raft
}

func (r *Raft) IsLeader() bool {
	return r.role == Leader
}

func (r *Raft) Leader() paxi.ID {
	return leaderID
}

func (r *Raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *Raft) sendHeartbeats(){
	ids:=config.IDs()
	for _,peerId := range ids{
		if peerId != r.ID(){
			var entries[]LogEntry
			rawMatchIndex,_ := r.matchIndex.Load(peerId)
			matchIndex, _ := rawMatchIndex.(int)
			if r.logLength-1 > matchIndex{
				rawNextIndex,_ := r.nextIndex.Load(peerId)
				nextIndex, _ := rawNextIndex.(int)
				entries = r.log[nextIndex:]
			}
			r.mu.Lock()
			m := AppendEntryArgs{
				Term:r.currentTerm,
				LeaderId:r.ID(),
				PrevLogIndex:r.logLength-2,
				LeaderCommit:r.commitIndex,
				Entries:entries,
			}
			if m.PrevLogIndex == -1 || m.PrevLogIndex == -2{
				m.PrevLogTerm = 0
				m.PrevLogIndex = -1
			}else{
				m.PrevLogTerm = r.log[m.PrevLogIndex].Term
			}
			go func(peerId paxi.ID){
				if r.role == Leader{
					r.mu.Unlock()
					r.Send(peerId,m)
				}else{
					r.mu.Unlock()
				}
			}(peerId)
		}
	}
}

func (r *Raft)RunLeader(){
	r.role = Leader
	leaderID = r.ID()

	for i:=r.commitIndex+1;i<r.logLength;i++{
		r.requestList.Store(i,r.log[i].Request)
	}

	ids := config.IDs()
	for _,peerId := range ids{
		r.nextIndex.Store(peerId,r.logLength)
		r.matchIndex.Store(peerId,-1)
	}
	log.Debugf("node %s is Leader",r.ID())
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			<-ticker.C
			if r.crash == false{
				r.sendHeartbeats()
			}
			r.mu.Lock()
			if r.role != Leader {
				loop := r.logLength - 1
				r.mu.Unlock()
				log.Debugf("changeLeader")
				for i:=r.commitIndex+1;i<loop;i++{
					rawRequest,ok := r.requestList.Load(i)
					if ok{
						request, _ := rawRequest.(paxi.Request)
						rep := paxi.Reply{
							Command:r.log[i].Command,
						}

						request.Reply(rep)
						r.requestList.Delete(i)
					}
				}
				
				return
			}
			r.mu.Unlock()

		}
	}()
}

func (r *Raft)runElectionTimer(){
	timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	if r.currentTerm == 0 && r.ID() != "1.1"{
		timeoutDuration = time.Duration(10000) * time.Millisecond
	}
	termStarted := r.currentTerm

	ticker:= time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for{
		<-ticker.C
		r.mu.Lock()
		if r.crash == true{
			r.mu.Unlock()
			//log.Infof("Node %v end runElectionTimer at Term %d because of crash",r.ID(),r.currentTerm)
			return
		}

		if r.role != Candidate && r.role != Follower{
			r.mu.Unlock()
			return
		}

		if termStarted != r.currentTerm {
			r.mu.Unlock()
			return
		}
		elapsed := time.Since(r.electionResetEvent)
		if elapsed >= timeoutDuration{
			
			r.mu.Unlock()
			r.StartElection()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Raft) StartElection(){
	r.mu.Lock()
	r.role = Candidate
	r.currentTerm += 1
	r.electionResetEvent = time.Now()
	r.votedFor= r.Node.ID()
	electionTerm := r.currentTerm
	log.Debugf("becomes Candidate (currentTerm=%d);", r.currentTerm)
	r.quorum.Reset()
	r.quorum.ACK(r.ID())
	r.mu.Unlock()
	m := RequestVoteArgs{
		Term: electionTerm,
		CandidateId: r.ID(),
		LogLength:r.logLength,
	}
	if m.LogLength > 0{
		m.LastEntryTerm = r.log[r.logLength-1].Term
	}
	r.Broadcast(m)

	go r.runElectionTimer()
}

func (r *Raft)RunFollower(currentTerm int){
	r.currentTerm = currentTerm
	r.votedFor = "-1"
	r.role = Follower
	r.electionResetEvent = time.Now()
	log.Infof("Node %v is follower at Term %d",r.ID(),r.currentTerm)
	go r.runElectionTimer()
}

func (r *Raft)HandleRequestVoteArgs(req RequestVoteArgs){
	//("receive request vote args")
	if req.Term > r.currentTerm{
		r.RunFollower(req.Term)
	}
	if req.Term == r.currentTerm && (r.votedFor == "-1" || r.votedFor == req.CandidateId ){
		m := RequestVoteReply{
			Term:r.currentTerm,
			VoteGranted:true,
			ID:r.ID(),
		}
		if r.logLength>0 && r.log[r.logLength-1].Term > req.LastEntryTerm{
			m.VoteGranted = false
			r.Send(req.CandidateId,m)
			return
		}else if r.logLength>0 && r.log[r.logLength-1].Term == req.LastEntryTerm{
			if r.logLength > req.LogLength{
				m.VoteGranted = false
				r.Send(req.CandidateId,m)
				return
			}
		}
		r.votedFor = req.CandidateId
		r.Send(req.CandidateId,m)
	} else{
		m := RequestVoteReply{
			Term:r.currentTerm,
			VoteGranted:false,
			ID:r.ID(),
		}
		r.Send(req.CandidateId,m)
	}
	
}
func (r *Raft)HandleRequestVoteReply(reply RequestVoteReply){
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role == Candidate{
		if reply.Term > r.currentTerm{
			r.mu.Unlock()
			r.RunFollower(reply.Term)
			r.mu.Lock()
		}

		if reply.VoteGranted && reply.Term == r.currentTerm{
			r.quorum.ACK(reply.ID)
			if r.quorum.Majority(){
				r.quorum.Reset()
				r.mu.Unlock()
				r.RunLeader()
				r.mu.Lock()
			}
		}

	}
}

func(r *Raft) checkConsistensy(m AppendEntryArgs) bool{
	if m.Term < r.currentTerm{
		return false
	}

	if m.PrevLogIndex == -1 {
		if r.logLength == 0{
			return true
		}else{
			//log.Debugf("333333")
			return false
		}
	}

	if m.PrevLogIndex > r.logLength - 1{
		//log.Debugf("44444")
		return false
	}

	if m.PrevLogTerm != r.log[m.PrevLogIndex].Term{
		//log.Debugf("555555")
		return false
	}

	return true
}

func (r *Raft)HandleAppendEntryArgs(m AppendEntryArgs){
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply AppendEntryReply
	if m.Entries != nil{
		//log.Debugf("node %s received AppendEntryArgs %+v", r.ID(), m)
		if r.checkConsistensy(m){
			consistentLog := r.log[:m.PrevLogIndex+1]
			r.log = append(consistentLog,m.Entries...)
			r.logLength = len(r.log)
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				LatestLogIndex:m.PrevLogIndex+len(m.Entries),
				NumEntries:len(m.Entries),
			}
			if m.LeaderCommit > r.commitIndex{
				if m.LeaderCommit > r.logLength - 1{
					r.commitIndex = r.logLength -1
				}else{
					r.commitIndex = m.LeaderCommit
				}
				
			}
			r.electionResetEvent = time.Now()
			r.votedFor = "-1"
			//log.Infof("Node %v has %v",r.ID(),r.log)
		}else{
			log.Debugf("not consistent. Follower latestIndex:%d. Leader prevlogIndex:%d,prevLogTerm:%d",r.logLength-1,m.PrevLogIndex,m.PrevLogTerm)
			reply = AppendEntryReply{
				Term:r.currentTerm,
				Success:false,
				ID:r.ID(),
				LatestLogIndex:r.logLength - 1,
				NumEntries:len(m.Entries),
			}

			if m.Term > r.currentTerm{
				reply.Term = m.Term
			}
		}
	}else{
		if m.Term > r.currentTerm{
			r.mu.Unlock()
			r.RunFollower(m.Term)
			r.mu.Lock()
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				NumEntries:len(m.Entries),
				LatestLogIndex:r.logLength-1,
			}
			leaderID = m.LeaderId
			return
		}
	
		if m.Term < r.currentTerm{
			reply = AppendEntryReply{
				Term:r.currentTerm,
				Success:false,
				ID:r.ID(),
				LatestLogIndex:r.logLength-1,
				NumEntries:len(m.Entries),
			}
		}
	
		if m.Term == r.currentTerm{
			if r.role != Follower{
				r.mu.Unlock()
				r.RunFollower(m.Term)
				r.mu.Lock()
			}
			r.electionResetEvent = time.Now()
			r.votedFor = "-1"
			prevLogIndex := r.logLength - 2
			if prevLogIndex == -2 {
				prevLogIndex = -1
			}
			if prevLogIndex != m.PrevLogIndex{
				reply = AppendEntryReply{
					Term:m.Term,
					Success:false,
					ID:r.ID(),
					NumEntries:len(m.Entries),
					LatestLogIndex:r.logLength-1,
				}
			}else{
				reply = AppendEntryReply{
					Term:m.Term,
					Success:true,
					ID:r.ID(),
					LatestLogIndex:r.logLength-1,
					NumEntries:len(m.Entries),
				}
				leaderID = m.LeaderId
				return
			}
		}
	}
	r.Send(m.LeaderId,reply)
}

func(r *Raft)advanceCommitIndex(){
	var commitIndexCandidates []int
	ids := config.IDs()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _,peerId := range ids{ 
		if peerId == r.ID(){
			commitIndexCandidates = append(commitIndexCandidates,r.logLength-1)
		}else{
			rawMatchIndex,_ := r.matchIndex.Load(peerId)
			matchIndex, _ := rawMatchIndex.(int)
			commitIndexCandidates = append(commitIndexCandidates,matchIndex)
		}
	}

	majority := (len(ids)+1)/2
	var newCommitIndex int
	for _,commitIndexCandidate := range commitIndexCandidates{
		var num int
		for _,otherCommitIndexCnadidates := range commitIndexCandidates{
			if commitIndexCandidate <= otherCommitIndexCnadidates{
				num += 1
			}
		}
		if num >= majority && commitIndexCandidate > newCommitIndex{
			newCommitIndex = commitIndexCandidate
		}
	}

	if r.commitIndex >= newCommitIndex{
		//log.Debugf("leader commitIndex:%d is larger than newCommitIndex:%d.",r.commitIndex,newCommitIndex)
		return
	}

	if newCommitIndex > r.logLength - 1{
	//	log.Debugf("there is no newCommitindex:%d in leaderLogIndex:%d",newCommitIndex,r.logLength-1)
		return
	}

	if r.log[newCommitIndex].Term != r.currentTerm{
		//log.Debugf("commitTerm:%d is not same as currentTerm:%d",r.log[newCommitIndex].Term, r.currentTerm)
		return
	}

	r.commitIndex = newCommitIndex

}
func (r *Raft)HandleAppendEntryReply(reply AppendEntryReply){
	r.mu.Lock()

	if r.role != Leader {
		r.mu.Unlock()
		return
	}

	if reply.Term > r.currentTerm{
		r.mu.Unlock()
		r.RunFollower(reply.Term)
		return
	}
	r.mu.Unlock()

	if reply.NumEntries == 0 && reply.Success{
		return
	}

	if reply.Success{
		r.matchIndex.Store(reply.ID,reply.LatestLogIndex)
		r.nextIndex.Store(reply.ID,reply.LatestLogIndex+1)
		r.mu.Lock()
		r.quorum.ACK(reply.ID)
		beforeCommitIndex := r.commitIndex
		r.mu.Unlock()
			if r.quorum.Majority(){
				r.quorum.Reset()
				r.advanceCommitIndex()
				r.mu.Lock()
				commitIndex := r.commitIndex
				//log.Debugf("beforeCommitIndex %d commitIndex %d",beforeCommitIndex,commitIndex)
				r.mu.Unlock()
				for i:=beforeCommitIndex+1;i<=commitIndex;i++{
						rawRequest,ok := r.requestList.Load(i)
					if !ok{
						//already executed
						//log.Debugf("already executed ")
						return
					}
					request, ok := rawRequest.(*paxi.Request)
					if !ok{
						//log.Debugf("ieuejddoej")
					}
					value := r.Execute(r.log[i].Command)
					//log.Debugf("commited:%v",r.log[r.commitIndex].Command)
					rep := paxi.Reply{
						Command:r.log[i].Command,
						Value:value,
					}
					
					request.Reply(rep)
					r.requestList.Delete(i)
				
				}
				//log.Debugf("reply has done.")
			}
	}else{
		r.mu.Lock()
		if reply.LatestLogIndex >= r.logLength{
			r.mu.Unlock()
			//log.Debugf("11111")
			return
		}
		r.mu.Unlock()
		r.nextIndex.Store(reply.ID,reply.LatestLogIndex+1)
		rawNextIndex,_ := r.nextIndex.Load(reply.ID)
		nextIndex, _ := rawNextIndex.(int)

		r.mu.Lock()
		logLength := r.logLength - 1
		r.mu.Unlock()
		if nextIndex > logLength {
			nextIndex = logLength
			r.nextIndex.Store(reply.ID,nextIndex)
		}
		if reply.LatestLogIndex == -1{
			//log.Debugf("2222 from %s",reply.ID)
			reply.LatestLogIndex = 0
		}
		r.mu.Lock()
		//log.Debugf("success false")
		m := AppendEntryArgs{
			Term:r.CurrentTerm(),
			LeaderId:r.Leader(),
			PrevLogIndex:reply.LatestLogIndex,
			PrevLogTerm:r.log[reply.LatestLogIndex].Term,
			Entries:r.log[nextIndex:],
			LeaderCommit:r.commitIndex,
			}
			r.mu.Unlock()

		r.Send(reply.ID,m)
		
		}
}

func (r *Raft)handleCrash(req paxi.Request){
	r.crash = true
	crashTime,_ := strconv.Atoi(string(req.Command.Value))
	log.Infof("Replica %s crash for %ds", r.ID(), crashTime)
	rep := paxi.Reply{
		Command:req.Command,
		Value:req.Command.Value,
	}
	req.Reply(rep)
	time.Sleep(time.Duration(crashTime) * time.Second)
	r.crash = false
}

func (r *Raft) HandleRequest(req paxi.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	//log.Debugf("Replica %s received reuest:%v\n", r.ID(), req)
	if r.IsLeader(){
		logEntry := LogEntry{
			Term:r.currentTerm,
			Command:req.Command,
			Request: &req,
		}
		r.log = append(r.log,logEntry)
		r.logLength += 1 
		r.mu.Unlock()
		r.requestList.Store(r.logLength-1,&req)
		r.mu.Lock()
		r.quorum.Reset()
		r.quorum.ACK(r.ID())
		m := AppendEntryArgs{
			Term:r.CurrentTerm(),
			LeaderId:r.Leader(),
			PrevLogIndex:r.logLength-2,
			Entries:r.log[r.logLength-1:],
			LeaderCommit:r.commitIndex,
		}
		if m.PrevLogIndex == -1{
			m.PrevLogTerm = 0
		}else{
			m.PrevLogTerm = r.log[m.PrevLogIndex].Term
		}
		r.Broadcast(m)
	//	log.Debugf("broadCast %v",m)
	}else{
		log.Debugf("This is not Leader\n")
		r.Forward(leaderID,req)
	}
}
