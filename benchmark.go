package paxi

import (
	"math"
	"math/rand"
	"sync"
	"time"
	"fmt"
	


	"github.com/ailidani/paxi/log"
)

// DB is general interface implemented by client to call client library
type DB interface {
	Init() error
	Read(key int) (int, error)
	Write(key, value int) error
	Stop() error
	Algorithm() string
	ClientID() string
}

// Bconfig holds all benchmark configuration
type Bconfig struct {
	T                    int     // total number of running time in seconds
	N                    int     // total number of requests
	K                    int     // key sapce
	W                    float64 // write ratio
	Throttle             int     // requests per second throttle, unused if 0
	Concurrency          int     // number of simulated clients
	Distribution         string  // distribution
	LinearizabilityCheck bool    // run linearizability checker at the end of benchmark
	// rounds       int    // repeat in many rounds sequentially

	// conflict distribution
	Conflicts int // percentage of conflicting keys
	Min       int // min key

	// normal distribution
	Mu    float64 // mu of normal distribution
	Sigma float64 // sigma of normal distribution
	Move  bool    // moving average (mu) of normal distribution
	Speed int     // moving speed in milliseconds intervals per key

	// zipfian distribution
	ZipfianS float64 // zipfian s parameter
	ZipfianV float64 // zipfian v parameter

	// exponential distribution
	Lambda float64 // rate parameter

	FaultyNode []ID
	CrashTime int
}

// DefaultBConfig returns a default benchmark config
func DefaultBConfig() Bconfig {
	return Bconfig{
		T:                    60,
		N:                    0,
		K:                    1000,
		W:                    0.5,
		Throttle:             0,
		Concurrency:          1,
		Distribution:         "uniform",
		LinearizabilityCheck: true,
		Conflicts:            100,
		Min:                  0,
		Mu:                   0,
		Sigma:                60,
		Move:                 false,
		Speed:                500,
		ZipfianS:             2,
		ZipfianV:             1,
		Lambda:               0.01,
		FaultyNode:			  []ID{},
		CrashTime:			  0,
	}
}
var history *History
// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	db DB // read/write operation interface
	Bconfig
	*History

	rate      *Limiter
	latency   []time.Duration // latency per operation
	startTime time.Time
	zipf      *rand.Zipf
	counter   int
	algorithm string

	wait sync.WaitGroup // waiting for all generated keys to complete
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(db DB) *Benchmark {
	b := new(Benchmark)
	b.db = db
	b.Bconfig = config.Benchmark
	b.History = NewHistory()
	history = b.History
	if b.Throttle > 0 {
		b.rate = NewLimiter(b.Throttle)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	r := rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	b.zipf = rand.NewZipf(r, b.ZipfianS, b.ZipfianV, uint64(b.K))
	log.Infof("Loaded config: %+v", config.Benchmark)
	return b
}

// Load will create all K keys to DB
func (b *Benchmark) Load() {
	b.W = 1.0
	b.Throttle = 0

	b.db.Init()
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	//go b.collect(latencies)

	b.startTime = time.Now()
	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}
	for i := b.Min; i < b.Min+b.K; i++ {
		b.wait.Add(1)
		keys <- i
	}
	//t := time.Now().Sub(b.startTime)

	b.db.Stop()
	close(keys)
	b.wait.Wait()
	//stat := Statistic(b.latency)

	// log.Infof("Benchmark took %v\n", t)
	// log.Infof("Throughput %f\n", float64(len(b.latency))/t.Seconds())
	// log.Info(stat)
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	admin := NewHTTPClient(ID("1.1"))

	var stop chan bool
	if b.Move {
		move := func() { b.Mu = float64(int(b.Mu+1) % b.K) }
		stop = Schedule(move, time.Duration(b.Speed)*time.Millisecond)
		defer close(stop)
	}

	// b.latency = make([]time.Duration, 0)
	// keys := make(chan int, b.Concurrency)
	// latencies := make(chan time.Duration, 1000)
	// defer close(latencies)
	// go b.collect(latencies)

	// for i := 0; i < b.Concurrency; i++ {
	// 	go b.worker(keys, latencies)
	// }

	algorithm := b.db.Algorithm()
	b.db.Init()
	if algorithm == "paxos"{
		k := rand.Intn(b.K) + b.Min // キーの計算

		var v int
		v = rand.Int()
		_ = b.db.Write(k, v)
	}
	b.startTime = time.Now()
if b.T > 0 {
    interval := time.Second / time.Duration(b.N) // リクエスト間隔
    totalRequests := b.T * b.N
    count := 0
	nextStart := time.Now() 
    for count < totalRequests {
        //start := time.Now() // 現在の時間を記録
		// 次の開始時刻までの時間を計算
		sleepDuration := time.Until(nextStart)
		if sleepDuration > 0 {
				time.Sleep(sleepDuration) // 必要に応じてスリープ
			}
		
				// 次のリクエストの予定時刻を更新
		nextStart = nextStart.Add(interval)
        //   op := new(operation)

        //     var s, e time.Time

		// 	k := rand.Intn(b.K) + b.Min 
		// 	s=time.Now()
		// 	e=time.Now()
		// 	op.start = s.Sub(b.startTime).Nanoseconds()
        //     op.end = e.Sub(b.startTime).Nanoseconds()
		// 	b.History.AddOperation(k, op)
        b.wait.Add(1)
		if count == totalRequests/2{
			for _,faultyNode := range b.FaultyNode{
				admin.Crash(faultyNode,b.CrashTime)
			}
		}
        go func() {
            defer b.wait.Done()

			k := rand.Intn(b.K) + b.Min // キーの計算
            op := new(operation)

            var s, e time.Time
            var err error
            var v int
			v = rand.Int()
            // 実際の処理開始
            s = time.Now()
            err = b.db.Write(k, v) // データベース書き込み処理
            e = time.Now()
			op.input = v

            // 結果の記録
            op.start = s.Sub(b.startTime).Nanoseconds()
            if err == nil {
                op.end = e.Sub(b.startTime).Nanoseconds()
            } else {
                op.end = math.MaxInt64
                log.Error(err)
            }
            b.History.AddOperation(k, op)
        }()
        count++
    }

    b.wait.Wait() // すべてのゴルーチンが終了するのを待機
}

	t := time.Now().Sub(b.startTime)

	// for i:=0;i<b.K;i++{
	// 	if !admin.Consensus(Key(i)){
	// 		fmt.Printf("No consensus on key = %d\n",i)
	// 		break
	// 	}
	// }
	b.db.Stop()
	//close(keys)
	//stat := Statistic(b.latency)
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Write Ratio = %f", b.W)
	log.Infof("Number of Keys = %d", b.K)
	log.Infof("Benchmark Time = %v\n", t)
	//log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	//log.Info(stat)

	//stat.WriteFile("latency")

	W:=b.W
	N:=b.N
	K:=b.K
	T:=b.T
	ID := b.db.ClientID()
	node := config.n
	faultyNode := len(b.FaultyNode) 
	b.History.WriteFile(algorithm,T,N,K,W,node,faultyNode,ID)

	// if b.LinearizabilityCheck {
	// 	n := b.History.Linearizable()
	// 	if n == 0 {
	// 		log.Info("The execution is linearizable.")
	// 	} else {
	// 		log.Info("The execution is NOT linearizable.")
	// 		log.Infof("Total anomaly read operations are %d", n)
	// 		log.Infof("Anomaly percentage is %f", float64(n)/float64(stat.Size))
	// 	}
	// }
}

// generates key based on distribution
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "order":
		b.counter = (b.counter + 1) % b.K
		key = b.counter + b.Min

	case "uniform":
		key = rand.Intn(b.K) + b.Min

	case "conflict":
		if rand.Intn(100) < b.Conflicts {
			key = 0
		} else {
			b.counter = (b.counter + 1) % b.K
			key = b.counter + b.Min
		}

	case "normal":
		key = int(rand.NormFloat64()*b.Sigma + b.Mu)
		for key < 0 {
			key += b.K
		}
		for key > b.K {
			key -= b.K
		}

	case "zipfan":
		key = int(b.zipf.Uint64())

	case "exponential":
		key = int(rand.ExpFloat64() / b.Lambda)

	default:
		log.Fatalf("unknown distribution %s", b.Distribution)
	}

	if b.Throttle > 0 {
		b.rate.Wait()
	}

	return key
}

func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	var wg sync.WaitGroup // 並列処理の待ち合わせ管理

	for k := range keys {
		now := time.Now()
		fmt.Printf("%s\n",now)
		wg.Add(1) // goroutineごとにカウントを追加
		go func(k int) {
			defer wg.Done() // goroutine完了時にカウントを減らす

			var s, e time.Time
			var v int
			var err error
			op := new(operation)

			// ノードクラッシュ・復帰処理
			if k == -1 {
				op.crash = "node has crashed "
			} else if k == -2 {
				op.crash = "node has returned "
			}

			// Write または Read の処理
			if rand.Float64() < b.W {
				v = rand.Int()
				s = time.Now()
				err = b.db.Write(k, v) // Write処理
				e = time.Now()
				op.input = v
			} else {
				s = time.Now()
				v, err = b.db.Read(k) // Read処理
				e = time.Now()
				op.output = v
			}

			// レイテンシを結果チャンネルに送信
			op.start = s.Sub(b.startTime).Nanoseconds()
			if err == nil {
				op.end = e.Sub(b.startTime).Nanoseconds()
				result <- e.Sub(s) // レイテンシ送信
			} else {
				op.end = math.MaxInt64
				log.Error(err)
			}

			// 履歴に操作を追加
			b.History.AddOperation(k, op)

		}(k) // goroutineにkを渡す
	}

	wg.Wait() // 全てのgoroutineが完了するのを待つ
}

// func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
// 	var s time.Time
// 	var e time.Time
// 	var v int
// 	var err error
// 	for k := range keys {
// 		op := new(operation)
// 		if k == -1{
// 			op.crash = "node"  + " has crashed "
// 		}else if k == -2{
// 			op.crash = "node" + " has returned "
// 		}
// 		if rand.Float64() < b.W {
// 			v = rand.Int()
// 			s = time.Now()
// 			//err = b.db.Write(k, v)
// 			e = time.Now()
// 			op.input = v
// 		} else {
// 			s = time.Now()
// 			//v, err = b.db.Read(k)
// 			e = time.Now()
// 			op.output = k
// 		}
// 		op.start = s.Sub(b.startTime).Nanoseconds()
// 		if err == nil {
// 			op.end = e.Sub(b.startTime).Nanoseconds()
// 			result <- e.Sub(s)
// 		} else {
// 			op.end = math.MaxInt64
// 		//	log.Error(err)
// 		}
// 		b.History.AddOperation(k, op)
// 	}
// }

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done()
	}
}
