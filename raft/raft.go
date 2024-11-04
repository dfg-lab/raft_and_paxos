package raft

import (

	"time"
	"sync"
	"math/rand"


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
	requestList []*paxi.Request
	mu sync.Mutex

	crash bool

}

var config paxi.Config
var leaderID paxi.ID


func NewRaft(n paxi.Node) *Raft {
	config = paxi.GetConfig()
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
	}
	return raft
}

func (r *Raft) IsLeader() bool {
	return r.role == Leader
}

func (r *Raft) Leader() paxi.ID {
	return r.Node.ID()
}

func (r *Raft) CurrentTerm() int {
	return r.currentTerm
}

func (r *Raft) sendHeartbeats(){
	ids:=config.IDs()
	for _,peerId := range ids{
		if peerId != r.ID(){
			m := AppendEntryArgs{
				Term:r.currentTerm,
				LeaderId:r.ID(),
			}
			go func(peerId paxi.ID){
				r.Send(peerId,m)
			}(peerId)
		}
	}
}

func (r *Raft)RunLeader(){
	log.Debugf("node %s is Leader",r.ID())
	r.role = Leader
	ids := config.IDs()
	for _,peerId := range ids{
		r.nextIndex[peerId] = len(r.log)
		r.matchIndex[peerId] = -1
	}
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		//startTime := time.Now()

		for {
			r.sendHeartbeats()
			<-ticker.C
			// elapsed := time.Since(startTime)
			// if elapsed > 10000*time.Millisecond && r.currentTerm == 1{
			// 	r.dlog("sleep")
			// 	r.crash = true
			// 	time.Sleep(time.Millisecond * 10000)
			// 	r.dlog("active")
			// 	r.crash = false
			// 	startTime = time.Now()
			// }

			// r.mu.Lock()
			// defer r.mu.Unlock()
			if r.role != Leader {
				log.Debugf("changeLeader")
				return
			}

		}
	}()
	// time.Sleep(time.Millisecond * 200)
	// r.becomeFollower((r.currentTerm))
}

func (r *Raft)runElectionTimer(){
	timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
	termStarted := r.currentTerm
	//r.dlog("election timerstarted (%v), term=%d", timeoutDuration, termStarted)

	ticker:= time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for{
		<-ticker.C
		r.mu.Lock()
		if r.role != Candidate && r.role != Follower{
			//r.dlog("in election timerstate=%s, bailing out", r.role)
			r.mu.Unlock()
			return
		}

		if termStarted != r.currentTerm {
			//r.dlog("in election timerterm changed from %d to %d, bailing out", termStarted, r.currentTerm)
			r.mu.Unlock()
			return
		}
		elapsed := time.Since(r.electionResetEvent)
		if elapsed >= timeoutDuration {
			//r.dlog("dosen't catch a heartbeat")
			r.StartElection()
			r.mu.Unlock()
			return
		}
		//r.dlog("nanionani,elapsed=(%v),r.electionResetEvent=(%v)",elapsed,r.electionResetEvent)
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
	//r.dlog("has peerIDs : %v",r.peerIds )
	//r.dlog("server has peerIDs : %v",r.server.PeerIds )
	r.quorum.Reset()
	r.quorum.ACK(r.ID())
	ids := config.IDs()
	for _,peerId := range ids{
		if peerId != r.ID(){
			go func(peerId paxi.ID){
				m := RequestVoteArgs{
					Term: electionTerm,
					CandidateId: r.ID(),
				}
				r.Send(peerId,m)
			}(peerId)
		}
	}

	go r.runElectionTimer()
	//r.dlog(" Run another election timer, in case this election is not successful at term %d",r.currentTerm)
}

func (r *Raft)RunFollower(currentTerm int){
	r.currentTerm = currentTerm
	r.votedFor = "-1"
	r.role = Follower
	r.electionResetEvent = time.Now()
	// for{
	// 	if !r.crash{
	// 		break
	// 	}
	// }
	log.Infof("Node %v is follower at Term %d",r.ID(),r.currentTerm)
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
		r.votedFor = req.CandidateId
		//r.dlog("vote for %s",r.votedFor)
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
	if m.PrevLogIndex == -1 && len(r.log) == 0{
		return true
	}

	if m.PrevLogIndex < len(r.log) - 1{
		return false
	}

	if m.PrevLogTerm != r.log[len(r.log)-1].Term{
		return false
	}

	return true
}

func (r *Raft)HandleAppendEntryArgs(m AppendEntryArgs){
	r.mu.Lock()
	defer r.mu.Unlock()
	var reply AppendEntryReply
	if m.Entries != nil{
		log.Debugf("node %s received message %+v", r.ID(), m)
		if r.checkConsistensy(m){
			consistentLog := r.log[:m.PrevLogIndex+1]
			r.log = append(consistentLog,m.Entries...)
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				PrevLogIndex:m.PrevLogIndex,
			}
		}else{
			reply = AppendEntryReply{
				Term:m.Term,
				Success:false,
				ID:r.ID(),
				PrevLogIndex:m.PrevLogIndex,
			}
		}
	}else{
		if m.Term > r.currentTerm{
			r.RunFollower(m.Term)
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				IsHeartBeat:true,
			}
			leaderID = m.LeaderId
		}
	
		if m.Term < r.currentTerm{
			reply = AppendEntryReply{
				Term:r.currentTerm,
				Success:false,
				ID:r.ID(),
				PrevLogIndex:m.PrevLogIndex,
				IsHeartBeat:true,
			}
		}
	
		if m.Term == r.currentTerm{
			if r.role != Follower{
				r.RunFollower(m.Term)
			}
			r.electionResetEvent = time.Now()
			r.votedFor = "-1"
			reply = AppendEntryReply{
				Term:m.Term,
				Success:true,
				ID:r.ID(),
				IsHeartBeat:true,
			}
			leaderID = m.LeaderId
		}
	}
	r.Send(m.LeaderId,reply)
}

func (r *Raft)HandleAppendEntryReply(reply AppendEntryReply){
	if reply.Term > r.currentTerm{
		r.RunFollower(reply.Term)
		return
	}

	if reply.IsHeartBeat{
		return
	}

	if reply.Success{
		r.nextIndex[reply.ID] += 1
		r.matchIndex[reply.ID] += 1
		r.quorum.ACK(reply.ID)
		r.mu.Lock()
			if r.quorum.Majority(){
				r.quorum.Reset()
				//commit
				r.commitIndex = reply.PrevLogIndex + 1
				log.Debugf("commit前")
				value := r.Execute(r.log[r.commitIndex].Command)
				log.Debugf("commited. log:%v",r.log)
				if r.requestList[r.commitIndex]!=nil{
					rep := paxi.Reply{
						Command:r.log[r.commitIndex].Command,
						Value:value,
					}
					r.requestList[r.commitIndex].Reply(rep)
					//reply.request = nil
				}
			}
		r.mu.Unlock()
	}else{
		r.nextIndex[reply.ID] -= 1
		ids := config.IDs()
		for _,peerId:= range ids{
			if peerId != r.ID(){
				go func(peerId paxi.ID){
					m := AppendEntryArgs{
						Term:r.CurrentTerm(),
						LeaderId:r.Leader(),
						PrevLogIndex:reply.PrevLogIndex-1,
						PrevLogTerm:r.log[reply.PrevLogIndex].Term,
						Entries:r.log[r.nextIndex[peerId]+1:],
						LeaderCommit:r.commitIndex,
					}
					log.Debugf("再送します")
					r.Send(peerId,m)
				}(peerId)
			}
		}
	}
}
func (r *Raft) HandleRequest(req paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), req)
	if r.IsLeader(){
		logEntry := LogEntry{
			Term:r.currentTerm,
			Command:req.Command,
		}
		r.log = append(r.log,logEntry)
		r.requestList = append(r.requestList,&req)
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
						Entries:r.log[r.nextIndex[peerId]:],
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

