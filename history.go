package paxi

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
	"path/filepath"
)

// History client operation history mapped by key
type History struct {
	sync.RWMutex
	shard      map[int][]*operation
	operations []*operation
}

// NewHistory creates a History map
func NewHistory() *History {
	return &History{
		shard:      make(map[int][]*operation),
		operations: make([]*operation, 0),
	}
}

// Add puts an operation in History
// func (h *History) Add(key int, input, output interface{}, start, end int64) {
// 	h.Lock()
// 	defer h.Unlock()
// 	if _, exists := h.shard[key]; !exists {
// 		h.shard[key] = make([]*operation, 0)
// 	}
// 	o := &operation{input, output, start, end}
// 	h.shard[key] = append(h.shard[key], o)
// 	h.operations = append(h.operations, o)
// }

// AddOperation adds the operation
func (h *History) AddOperation(key int, o *operation) {
	h.Lock()
	defer h.Unlock()
	if _, exists := h.shard[key]; !exists {
		h.shard[key] = make([]*operation, 0)
	}
	h.shard[key] = append(h.shard[key], o)
	h.operations = append(h.operations, o)
}

// Linearizable concurrently checks if each partition of the history is linearizable and returns the total number of anomaly reads
func (h *History) Linearizable() int {
	anomalies := make(chan []*operation)
	h.RLock()
	defer h.RUnlock()
	for _, partition := range h.shard {
		c := newChecker()
		go func(p []*operation) {
			anomalies <- c.linearizable(p)
		}(partition)
	}
	sum := 0
	for range h.shard {
		a := <-anomalies
		sum += len(a)
	}
	return sum
}

// WriteFile writes entire operation history into file
func (h *History) WriteFile(algorithm string,T int,N int,K int,W float64,node int,faultyNode int,ID string) error {
	now := time.Now()
	yearMonthDay := now.Format("20060102")
	folderPath := filepath.Join(".", yearMonthDay)
	err := os.MkdirAll(folderPath, os.ModePerm)
    if err != nil {
        fmt.Println("Error creating directory:", err)
        return err
    }
	
	time := now.Format("20060102_150405")
	t:= strconv.Itoa(T)
	n:= strconv.Itoa(N)
	k:= strconv.Itoa(K)
	wr:= strconv.FormatFloat(W, 'f', -1, 64)
	FaultyNode := strconv.Itoa(faultyNode)
	Node:=strconv.Itoa(node)
	fileName := algorithm +"_T"+t+"_N"+n+"_K"+k+"_W"+wr+"_Node"+Node+"_faultyNode"+FaultyNode+"_"+ time + ".csv"
	filePath := filepath.Join(folderPath, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	h.RLock()
	defer h.RUnlock()

	sort.Sort(byTime(h.operations))

	latency := 0.0
	throughput := 0
	//s := 1.0
	fmt.Fprintf(w,"this client connects to %s\n",ID)
	fmt.Fprintf(w, "Input,Output,startTime(s),endTime(s)\n")
	for _, o := range h.operations {
		if o.crash ==nil{
			start := float64(o.start) / 1000000000.0
			end := float64(o.end) / 1000000000.0
			fmt.Fprintf(w, "%v,%v,%f,%f\n", o.input, o.output, start, end)
			latency += end - start
			throughput++
			// if end > s {
			// 	fmt.Fprintf(w, "PerSecond %f %d\n", latency/float64(throughput)*1000.0, throughput)
			// 	latency = 0
			// 	throughput = 0
			// 	s++
			// }
		}else{
			fmt.Fprintf(w,"%s\n",o.crash)
		}

		// fmt.Fprintln(w, o)
	}

	// for k, ops := range h.shard {
	// 	fmt.Fprintf(w, "key=%d\n", k)
	// 	for _, o := range ops {
	// 		fmt.Fprintln(w, o)
	// 	}
	// }
	return w.Flush()
}

// ReadFile reads csv log file and create operations in history
func (h *History) ReadFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}

	r := csv.NewReader(file)

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(record) < 5 {
			return errors.New("operation history file format error")
		}

		// get id / key
		id, err := strconv.Atoi(record[0])
		if err != nil {
			return err
		}

		operation := new(operation)

		// get input
		if record[1] == "null" || record[1] == "" {
			operation.input = nil
		} else {
			operation.input = record[1]
		}

		// get output
		if record[2] == "null" || record[2] == "" {
			operation.output = nil
		} else {
			operation.output = record[2]
		}

		// get start time
		start, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			log.Fatal(err)
			return err
		}
		operation.start = start

		// get end time
		end, err := strconv.ParseInt(record[4], 10, 64)
		if err != nil {
			log.Fatal(err)
			return err
		}
		operation.end = end

		h.AddOperation(id, operation)
	}

	return file.Close()
}
