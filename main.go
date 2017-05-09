package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pborman/uuid"
)

const (
	// DefaultDatasetSize is the default number of points to send in a given
	// bulk request
	DefaultDatasetSize = 768
	// DefaultServiceCount is the number of services to use in the tags
	DefaultServiceCount = 10
	// DefaultHostCount is the number of hosts to use in the tags
	DefaultHostCount = 100
	// DefaultKeyspace is the default keyspace to send data to
	DefaultKeyspace = "stats"
)

var (
	nonce string
)

// SenderFactory creates sender objects
type SenderFactory func(id uint64, servers []string, port uint16) Sender

// Sender defines the behaviour of how objects send data to OpenTSDB servers
type Sender interface {
	Send(DataList) error
}

var (
	// Protocols are the functions that send data
	Protocols = map[string]SenderFactory{
		"http": func(id uint64, servers []string, port uint16) Sender {
			return &restSender{id: id, servers: servers, port: port}
		},
		"udp": func(id uint64, servers []string, port uint16) Sender {
			return &udpSender{id: id, servers: servers, port: port}
		},
	}

	// Logger is the default logger
	Logger *logrus.Logger
)

func init() {
	Logger = &logrus.Logger{
		Out:       os.Stdout,
		Formatter: &logrus.TextFormatter{},
		Level:     logrus.InfoLevel,
	}

	nonce = uuid.New()
}

// RunTest actually runs the test
func RunTest(datasetSize, hostCount, serviceCount uint64, keyspace string, sender Sender) error {
	var host, service, start, delta uint64
	data := make(DataList, datasetSize)
	for index := range data {
		data[index].Metric = fmt.Sprintf("testing.metric.run-%s.index-%d", nonce, index)
	}
	start = uint64(rand.Int63n(int64(hostCount)))
	for delta = 0; delta <= hostCount; delta++ {
		host = ((start + delta) % hostCount) + 1
		for service = 1; service <= serviceCount; service++ {
			for index := range data {
				data[index].ChangeDefaultTags(
					fmt.Sprintf("host-%s-%d", nonce, host),
					fmt.Sprintf("service-%s-%d", nonce, service),
					keyspace,
				)
				data[index].Randomize()
			}
			if err := sender.Send(data); err != nil {
				Logger.WithFields(logrus.Fields{
					"service": service,
					"host":    host,
				}).Errorf("Error: %s\n", err.Error())
			}
		}
	}
	return nil
}

func main() {
	var (
		datasetSize  uint64
		hostCount    uint64
		serviceCount uint64
		goroutines   uint64
		port         uint64
		sender       string
		keyspace     string
		debug        bool

		wg sync.WaitGroup
	)

	rand.Seed(time.Now().UnixNano())

	flag.Uint64Var(&datasetSize, "dataset", DefaultDatasetSize,
		"Number of points to send related to each combination service-host")
	flag.Uint64Var(&hostCount, "hosts", DefaultHostCount,
		"Number of hosts to use in the tests")
	flag.Uint64Var(&serviceCount, "services", DefaultServiceCount,
		"Number of services to use in the tests")
	flag.Uint64Var(&port, "port", 8080,
		"Change the default port")
	flag.StringVar(&sender, "protocol", "http",
		"Method to use when sending data")
	flag.StringVar(&keyspace, "keyspace", DefaultKeyspace,
		"Keyspace to send data to")
	flag.Uint64Var(&goroutines, "go", 1,
		"Number of goroutines to start")
	flag.BoolVar(&debug, "debug", false,
		"Forces the logger to go into debug mode")
	flag.Parse()

	if debug {
		Logger.Level = logrus.DebugLevel
	}

	servers := flag.Args()
	if len(servers) <= 0 {
		Logger.Errorf("Servers with OpenTSDB are required as positional arguments\n")
		return
	}

	factory, ok := Protocols[sender]
	if !ok {
		Logger.Errorf("Method not recognized: %s", sender)
		return
	}

	wg.Add(int(goroutines))
	for i := uint64(1); i <= goroutines; i++ {
		go func(i uint64) {
			Logger.WithFields(debugFields("main", "", "main")).Debugf("Starting goroutine: %d", i)
			for {
				RunTest(datasetSize, hostCount, serviceCount, keyspace, factory(i, servers, uint16(port)))
			}
			// wg.Done()
		}(i)
	}
	wg.Wait()
}
