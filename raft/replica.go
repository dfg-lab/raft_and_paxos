package raft

import (


	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/log"
)




// Replica for one Paxos instance
type Replica struct {
	paxi.Node
	*Raft
}

// NewReplica generates new Paxos replica
func NewReplica(id paxi.ID) *Replica {
	r := new(Replica)
	r.Node = paxi.NewNode(id)
	r.Raft = NewRaft(r)
	r.Register(paxi.Request{}, r.handleRequest)
	r.Register(RequestVoteArgs{}, r.HandleRequestVoteArgs)
	r.Register(RequestVoteReply{}, r.HandleRequestVoteReply)
	r.Register(AppendEntryArgs{}, r.HandleAppendEntryArgs)
	r.Register(AppendEntryReply{}, r.HandleAppendEntryReply)
	return r
}

func(r *Replica)Run(){
	log.Infof("node %s start running", r.ID())
	r.Raft.RunFollower(0)
	r.Node.Run()
}

func (r *Replica) handleRequest(m paxi.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if r.Raft.IsLeader(){
		r.Raft.HandleRequest(m)
	} else {
		go r.Forward(r.Raft.Leader(), m)
	}
}


