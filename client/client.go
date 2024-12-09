package main

import (
	"encoding/binary"
	"flag"


	"github.com/ailidani/paxi"
	"github.com/ailidani/paxi/chain"
	"github.com/ailidani/paxi/paxos"

)

var id = flag.String("id", "", "node id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	paxi.Client
	algorithm string
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := paxi.Key(k)
	v, err := d.Get(key)
	if len(v) == 0 {
		return 0, nil
	}
	x, _ := binary.Uvarint(v)
	return int(x), err
}

func (d *db) Write(k, v int) error {
	key := paxi.Key(k)
	value := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(value, uint64(v))
	err := d.Put(key, value)
	return err
}

func (d *db)Algorithm() string{
	return *algorithm
}

func (d *db)ClientID() string{
	return *id
}

func main() {
	paxi.Init()

	if *master != "" {
		paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	}

	d := new(db)
	switch *algorithm {
	case "paxos":
		d.Client = paxos.NewClient(paxi.ID(*id))
	case "chain":
		d.Client = chain.NewClient()
	default:
		d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	}

	b := paxi.NewBenchmark(d)
	if *load {
		b.Load()
	} else {
		b.Run()
	// 	var wg sync.WaitGroup
	// 	wg.Add(1)

	// 	go func(){
	// 		defer wg.Done()
	// 		b.Run()
	// 	}()

	// 	crashTimer := time.NewTimer(time.Second*5)
	// 	select{
	// 	case <- crashTimer.C:
	// 		if adminClient, ok := d.Client.(paxi.AdminClient); ok {
	// 			//adminClient.Crash("1.2", 2)
	// 			log.Debugf("%v",adminClient)
	// 		} else {
	// 			log.Debugf("Client does not support admin operations")
	// 		}
			
	// 	}
	// 	wg.Wait()
	}
}
