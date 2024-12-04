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
	nextIndex map[paxi.ID]int
	matchIndex map[paxi.ID]int

	role Role
	electionResetEvent time.Time
	quorum *paxi.Quorum
	requestList map[int]*paxi.Request
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
		nextIndex:make(map[paxi.ID]int),
		matchIndex:make(map[paxi.ID]int),
		quorum:paxi.NewQuorum(),
		requestList:make(map[int]*paxi.Request),
	}
	for _,peerId := range ids{
		raft.matchIndex[peerId] = -1
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
	r.mu.Lock()
	defer r.mu.Unlock()
	ids:=config.IDs()
	for _,peerId := range ids{
		if peerId != r.ID(){
			var entries[]LogEntry
			if len(r.log)-1 > r.matchIndex[peerId]{
				entries = r.log[r.nextIndex[peerId]:]
				log.Debugf("リーダーのログがフォロワーより多い %v",entries)
			}
			m := AppendEntryArgs{
				Term:r.currentTerm,
				LeaderId:r.ID(),
				PrevLogIndex:len(r.log)-2,
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
					r.Send(peerId,m)
				}
			}(peerId)
		}
	}
}

func (r *Raft)RunLeader(){
	log.Debugf("node %s is Leader",r.ID())
	r.role = Leader
	leaderID = r.ID()

	for i:=r.commitIndex+1;i<len(r.log);i++{
		r.requestList[i] = r.log[i].Request
	}

	ids := config.IDs()
	for _,peerId := range ids{
		r.nextIndex[peerId] = len(r.log)
		r.matchIndex[peerId] = -1
	}
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
				r.mu.Unlock()
				log.Debugf("changeLeader")
				for i:=r.commitIndex+1;i<len(r.log)-1;i++{
					if r.requestList[i].Properties["reply"]=="false"{
						rep := paxi.Reply{
							Command:r.log[i].Command,
						}
						request := r.requestList[i]
						request.Reply(rep)
						request.Properties["reply"]="true"
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
		if elapsed >= timeoutDuration {
			r.StartElection()
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
	}
}

func (r *Raft) StartElection(){
	r.role = Candidate
	r.currentTerm += 1
	r.electionResetEvent = time.Now()
	r.votedFor= r.Node.ID()
	electionTerm := r.currentTerm
	log.Debugf("becomes Candidate (currentTerm=%d);", r.currentTerm)
	r.quorum.Reset()
	r.quorum.ACK(r.ID())
	ids := config.IDs()
	for _,peerId := range ids{
		if peerId != r.ID(){
			go func(peerId paxi.ID){
				m := RequestVoteArgs{
					Term: electionTerm,
					CandidateId: r.ID(),
					LogLength:len(r.log),
				}
				if m.LogLength > 0{
					m.LastEntryTerm = r.log[len(r.log)-1].Term
				}
				r.Send(peerId,m)
			}(peerId)
		}
	}

	go r.runElectionTimer()
}

func (r *Raft)RunFollower(currentTerm int){
	if currentTerm == 0 && r.ID() != "1.1"{
		log.Debugf("休んでいます")
		time.Sleep(time.Duration(1)*time.Second)
	}
	r.currentTerm = currentTerm
	r.votedFor = "-1"
	r.role = Follower
	r.electionResetEvent = time.Now()
	log.Infof("Node %v is follower at Term %d",r.ID(),r.currentTerm)
	log.Infof("Node %v has %v",r.ID(),r.log)
	//fmt.Printf("is follower at Term %d",r.currentTerm)
	go r.runElectionTimer()
}

func (r *Raft)HandleRequestVoteArgs(req RequestVoteArgs){
	if req.Term > r.currentTerm{
		r.RunFollower(req.Term)
	}
	if req.Term == r.currentTerm && (r.votedFor == "-1" || r.votedFor == req.CandidateId ){
		m := RequestVoteReply{
			Term:r.currentTerm,
			VoteGranted:true,
			ID:r.ID(),
		}
		if len(r.log)>0 && r.log[len(r.log)-1].Term > req.LastEntryTerm{
			m.VoteGranted = false
			r.Send(req.CandidateId,m)
			return
		}else if len(r.log)>0 && r.log[len(r.log)-1].Term == req.LastEntryTerm{
			if len(r.log) > req.LogLength{
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
			r.RunFollower(reply.Term)
		}

		if reply.VoteGranted && reply.Term == r.currentTerm{
			r.quorum.ACK(reply.ID)
			if r.quorum.Majority(){
				r.quorum.Reset()
				r.RunLeader()
			}
		}

	}
}

func(r *Raft) checkConsistensy(m AppendEntryArgs) bool{
	if m.Term < r.currentTerm{
		return false
	}

	if m.PrevLogIndex == -1 {
		if len(r.log) == 0{
			return true
		}else{
			return false
		}
	}

	if m.PrevLogIndex > len(r.log) - 1{
		return false
	}

	if m.PrevLogTerm != r.log[m.PrevLogIndex].Term{
		return false
	}

	return true
}

func (r *Raft)HandleAppendEntryArgs(m AppendEntryArgs){
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply AppendEntryReply
	if m.Entries != nil{
		log.Debugf("node %s received AppendEntryArgs %+v", r.ID(), m)
		if r.checkConsistensy(m){
			consistentLog := r.log[:m.PrevLogIndex+1]
			r.log = append(consistentLog,m.Entries...)
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				LatestLogIndex:m.PrevLogIndex+len(m.Entries),
				NumEntries:len(m.Entries),
			}
			if m.LeaderCommit > r.commitIndex{
				if m.LeaderCommit > len(r.log) - 1{
					r.commitIndex = len(r.log) -1
				}else{
					r.commitIndex = m.LeaderCommit
				}
				
			}
			r.electionResetEvent = time.Now()
			r.votedFor = "-1"
			//log.Infof("Node %v has %v",r.ID(),r.log)
		}else{
			//log.Debugf("not consistent. Follower latestIndex:%d,prevLogTerm:%d. Leader prevlogIndex:%d,prevLogTerm:%d",len(r.log)-1,r.log[len(r.log)-1].Term,m.PrevLogIndex,m.PrevLogTerm)
			reply = AppendEntryReply{
				Term:r.currentTerm,
				Success:false,
				ID:r.ID(),
				LatestLogIndex:len(r.log) - 1,
				NumEntries:len(m.Entries),
			}

			if m.Term > r.currentTerm{
				reply.Term = m.Term
			}
			// if reply.LatestLogIndex == -2{
			// 	reply.LatestLogIndex = -1
			// }
		}
	}else{
		if m.Term > r.currentTerm{
			r.RunFollower(m.Term)
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				NumEntries:len(m.Entries),
				LatestLogIndex:len(r.log)-1,
			}
			leaderID = m.LeaderId
		}
	
		if m.Term < r.currentTerm{
			reply = AppendEntryReply{
				Term:r.currentTerm,
				Success:false,
				ID:r.ID(),
				LatestLogIndex:len(r.log)-1,
				NumEntries:len(m.Entries),
			}
			//log.Debugf("leader term is not latest")
		}
	
		if m.Term == r.currentTerm{
			if r.role != Follower{
				r.RunFollower(m.Term)
			}
			r.electionResetEvent = time.Now()
			r.votedFor = "-1"
			prevLogIndex := len(r.log) - 2
			if prevLogIndex == -2 {
				prevLogIndex = -1
			}
			if prevLogIndex != m.PrevLogIndex{
			//	log.Debugf("node %s received appendEntryArgs %+v", r.ID(), m)
				//log.Debugf("mismatch logIndex. Request for retransimission. This node had %d entries. m.PrevLogIndex:%d",len(r.log),m.PrevLogIndex)
				reply = AppendEntryReply{
					Term:m.Term,
					Success:false,
					ID:r.ID(),
					NumEntries:len(m.Entries),
					LatestLogIndex:len(r.log)-1,
				}
			}else{
				reply = AppendEntryReply{
					Term:m.Term,
					Success:true,
					ID:r.ID(),
					LatestLogIndex:len(r.log)-1,
					NumEntries:len(m.Entries),
				}
				leaderID = m.LeaderId
			}
		}
	}
	//log.Debugf("send :%+v",reply)
	r.Send(m.LeaderId,reply)
}

func(r *Raft)advanceCommitIndex(){
	var commitIndexCandidates []int
	ids := config.IDs()
	for _,peerId := range ids{ 
		if peerId == r.ID(){
			commitIndexCandidates = append(commitIndexCandidates,len(r.log)-1)
		}else{
			commitIndexCandidates = append(commitIndexCandidates,r.matchIndex[peerId])
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

	if newCommitIndex > len(r.log) - 1{
		//log.Debugf("there is no newCommitindex:%d in leaderLogIndex:%d",newCommitIndex,len(r.log)-1)
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
		r.RunFollower(reply.Term)
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	if reply.NumEntries == 0 && reply.Success{
		return
	}

	if reply.Success{
		r.mu.Lock()
		r.matchIndex[reply.ID] = reply.LatestLogIndex
		r.nextIndex[reply.ID] = reply.LatestLogIndex + 1
		r.quorum.ACK(reply.ID)
		//log.Debugf("nextIndex[%s]=%d",reply.ID,r.nextIndex[reply.ID])
			if r.quorum.Majority(){
				r.quorum.Reset()
				//advance commitIndex
				r.advanceCommitIndex()
				//log.Debugf("commit前")
				if r.requestList[r.commitIndex]!=nil&&r.requestList[r.commitIndex].Properties["reply"]=="true"{
					r.mu.Unlock()
					return
				}
				value := r.Execute(r.log[r.commitIndex].Command)
				//log.Debugf("commited:%v",r.log[r.commitIndex].Command)
				//log.Infof("Node %v has %v",r.ID(),r.log)
				if r.requestList[r.commitIndex]!=nil{
					rep := paxi.Reply{
						Command:r.log[r.commitIndex].Command,
						Value:value,
					}
					request := r.requestList[r.commitIndex]
					request.Reply(rep)
					request.Properties["reply"]="true"
					
					//reply.request = nil
					//log.Debugf("reply has done.")
				}
			}
		r.mu.Unlock()
	}else{
		// add r.currentterm < rep.currentterm
		r.mu.Lock()
		r.nextIndex[reply.ID] = reply.LatestLogIndex + 1
		log.Debugf("nextIndex[%s]=%d",reply.ID,r.nextIndex[reply.ID])
		log.Debugf("received:%+v",reply)
		m := AppendEntryArgs{
			Term:r.CurrentTerm(),
			LeaderId:r.Leader(),
			PrevLogIndex:reply.LatestLogIndex,
			PrevLogTerm:r.log[reply.LatestLogIndex].Term,
			Entries:r.log[r.nextIndex[reply.ID]:],//slice bounds out of range [7:6]
			LeaderCommit:r.commitIndex,
			}
		//log.Debugf("retransimission. received appendEntryReply:%v",reply)
		r.Send(reply.ID,m)
		r.mu.Unlock()
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
	//r.RunFollower(r.currentTerm)
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
		logEntry.Request.Properties["reply"] = "false"
		r.log = append(r.log,logEntry)
		r.requestList[len(r.log)-1] = &req
		r.quorum.Reset()
		r.quorum.ACK(r.ID())
		ids := config.IDs()
		for _,peerId:= range ids{
			if peerId != r.ID(){
				go func(peerId paxi.ID){
					m := AppendEntryArgs{
						Term:r.CurrentTerm(),
						LeaderId:r.Leader(),
						PrevLogIndex:len(r.log)-2,
						//PrevLogTerm:r.log[len(r.log)-1].Term,
						Entries:r.log[len(r.log)-1:],
						LeaderCommit:r.commitIndex,
					}
					if m.PrevLogIndex == -1{
						m.PrevLogTerm = 0
					}else{
						m.PrevLogTerm = r.log[m.PrevLogIndex].Term
					}
					r.Send(peerId,m)
				}(peerId)
			}
			
		}
	}else{
		log.Debugf("This is not Leader\n")
		r.Forward(leaderID,req)
	}
}

