package raft

import (
	"encoding/gob"


	"github.com/ailidani/paxi"
)

func init() {
	gob.Register(RequestVoteArgs{})
	gob.Register(RequestVoteReply{})
	gob.Register(AppendEntryArgs{})
	gob.Register(AppendEntryReply{})
}

type RequestVoteArgs struct{
	Term int
	CandidateId paxi.ID
	LogLength int
	LastEntryTerm int
}

type RequestVoteReply struct{
	Term int
	VoteGranted bool
	ID paxi.ID
}

type LogEntry struct{
	Term int
	Command paxi.Command
	Request *paxi.Request
	quorum *paxi.Quorum
}

type AppendEntryArgs struct{
	Term     int
	LeaderId paxi.ID

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntryReply struct{
	Term int
	Success bool
	ID paxi.ID
	LatestLogIndex int
	NumEntries int
}


