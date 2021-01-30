package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func main() {
	log.Print("Starting...")
	host := flag.String("host", "127.0.0.1", "ブローカーホスト名")
	port := flag.String("port", "1883", "ブローカーポート番号")
	clientNum := flag.Int("clients", 1, "クライアント数")
	waitSec := flag.Int("waitsec", 1, "Publisherからの終了シグナルを受信してから、実際にSubscribeを終了するまでの秒数")
	flag.Parse()

	// オプションの表示
	log.Printf("OPTION Broker hostname            : %v", *host)
	log.Printf("OPTION Broker port                : %v", *port)
	log.Printf("OPTION Client num                 : %v", *clientNum)
	log.Printf("OPTION Wait time                  : %v [sec]", *waitSec)

	clients := make([]mqtt.Client, *clientNum)
	log.Print("Allocated!!!")
	for i := 0; i < *clientNum; i++ {
		// ゲートウェイブローカへ接続
		opts := mqtt.NewClientOptions()
		opts.AddBroker(fmt.Sprintf("tcp://%s:%s", *host, *port))
		c := mqtt.NewClient(opts)
		if token := c.Connect(); token.Wait() && token.Error() != nil {
			log.Fatalf("MQTT Connect error: %s", token.Error())
		}
		log.Printf("Client counter: %v", i+1)
		clients[i] = c
	}
	defer func(clients []mqtt.Client) {
		for i, c := range clients {
			c.Disconnect(500)
			log.Printf("Disconnecting... (%v)", i)
		}
	}(clients)

	metrics := NewMetrics()
	log.Print("Starting goroutine...")
	for i := 0; i < *clientNum; i++ {
		var measurementHandler mqtt.MessageHandler = func(c mqtt.Client, m mqtt.Message) {
			var msg PayloadMeasurement
			if err := json.Unmarshal(m.Payload(), &msg); err != nil {
				log.Fatal(err)
			}
			metric := metrics.GetOrCreate(msg.ID)
			metric.Add(msg.TimeMs)
		}

		var signalHandler mqtt.MessageHandler = func(c mqtt.Client, m mqtt.Message) {
			var msg PayloadSignal
			if err := json.Unmarshal(m.Payload(), &msg); err != nil {
				log.Fatal(err)
			}
			metric := metrics.GetOrCreate(msg.ID)

			if !msg.IsDone || metric.GetIsDone() {
				return
			}
			log.Printf("Waiting %v seconds...", *waitSec)
			time.Sleep(time.Second * time.Duration(*waitSec))
			metrics.SetIsDone(msg.ID)
		}

		go sub(clients[i], measurementHandler, signalHandler)
	}
	log.Print("Done launching goroutine.")
	time.Sleep(time.Second)

	doneCh := make(chan bool)
	go func() {
		now := time.Now().Unix()
		for {
			averageList := metrics.GetAverageList()
			if len(averageList) == 0 {
				if int(time.Now().Unix()-now) > *waitSec {
					metrics.SetDoneAll()
					break
				}
				continue
			}
			for _, a := range averageList {
				log.Printf("Average : %v [ms] [n=%v] (ID: %v)", a.Average, a.N, a.Id)
			}
			time.Sleep(time.Millisecond * 500)
			now = time.Now().Unix()
		}
		time.Sleep(time.Second)
		averageList := metrics.GetAverageList()
		for _, a := range averageList {
			log.Printf("Average : %v [ms] [n=%v] (ID: %v)", a.Average, a.N, a.Id)
		}
		doneCh <- true
	}()

	//////////////           勝手に終了しないようにする          //////////////
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, os.Kill)
	for {
		select {
		case <-signalCh:
			log.Print("Interrupt detected.")
			return
		case <-doneCh:
			log.Print("Finished measurement.")
			return
		}
	}
}

func sub(c mqtt.Client, measurementHandler mqtt.MessageHandler, signalHandler mqtt.MessageHandler) {
	if token := c.Subscribe("/0/#", 0, measurementHandler); token.Wait() && token.Error() != nil {
		log.Printf("MQTT Subscribe error")
		return
	}
	if token := c.Subscribe("/1/#", 0, measurementHandler); token.Wait() && token.Error() != nil {
		log.Printf("MQTT Subscribe error")
		return
	}
	if token := c.Subscribe("/2/#", 0, measurementHandler); token.Wait() && token.Error() != nil {
		log.Printf("MQTT Subscribe error")
		return
	}
	if token := c.Subscribe("/3/#", 0, measurementHandler); token.Wait() && token.Error() != nil {
		log.Printf("MQTT Subscribe error")
		return
	}
	if token := c.Subscribe("/signal", 0, signalHandler); token.Wait() && token.Error() != nil {
		log.Printf("MQTT Subscribe error")
		return
	}
}

type PayloadSignal struct {
	ID     string `json:"id"`
	TimeMs int64  `json:"time_ms"`
	IsDone bool   `json:"is_done"`
}

type PayloadMeasurement struct {
	ID      string `json:"id"`
	TimeMs  int64  `json:"time_ms"`
	Padding string `json:"padding"`
}

func int2Topicname(t uint64, level int) string {
	mask := uint64(0b1100000000000000000000000000000000000000000000000000000000000000)
	topic := ""
	for i := 0; i < level && i < 32; i++ {
		topic += fmt.Sprintf("/%v", uint64((t&mask)>>(62-i*2)))
		mask = mask >> 2
	}
	return topic
}

type Average struct {
	Id      string
	Average float64
	N       int64
}

type Metrics struct {
	sync.RWMutex
	metrics map[string]*Metric
	num     int
}

func NewMetrics() *Metrics {
	return &Metrics{metrics: map[string]*Metric{}, num: -1}
}

func (ms *Metrics) SetIsDone(id string) bool {
	ms.Lock()
	defer ms.Unlock()
	m, ok := ms.metrics[id]
	if !ok {
		return false
	}
	if m.GetIsDone() {
		return false
	}
	m.setDone()
	ms.num--
	log.Printf("Done (ID: %v)", id)
	return true
}

func (ms *Metrics) SetDoneAll() {
	ms.RLock()
	defer ms.RUnlock()
	for _, m := range ms.metrics {
		m.setDone()
	}
}

func (ms *Metrics) GetAverageList() []Average {
	ms.RLock()
	defer ms.RUnlock()
	result := []Average{}
	i := 0
	for _, m := range ms.metrics {
		ok, average := m.Average()
		if !ok {
			continue
		}
		result = append(result, average)
		i++
	}
	return result
}

func (ms *Metrics) IsDoneAll() bool {
	ms.RLock()
	defer ms.RUnlock()
	return ms.num == 0
}

func (ms *Metrics) GetOrCreate(id string) *Metric {
	m, ok := ms.get(id)
	if !ok {
		m = ms.create(id)
	}
	return m
}

func (ms *Metrics) get(id string) (*Metric, bool) {
	ms.RLock()
	defer ms.RUnlock()
	m, ok := ms.metrics[id]
	return m, ok
}

func (ms *Metrics) create(id string) *Metric {
	ms.Lock()
	defer ms.Unlock()
	m := NewMetric(id)
	ms.metrics[id] = m
	if ms.num < 0 {
		ms.num = 1
	} else {
		ms.num++
	}
	return m
}

type Metric struct {
	sync.RWMutex
	id          string
	time        int64
	average     float64
	averageN    int64
	averageRead bool
	counter     int64
	counterRead bool
	sum         int64
	isDone      bool
}

func NewMetric(id string) *Metric {
	return &Metric{time: time.Now().Unix(), isDone: false, averageRead: false, counterRead: false, id: id}
}

func (m *Metric) Add(t int64) {
	m.Lock()
	defer m.Unlock()
	if m.isDone {
		log.Printf("[Warning] Already ended this process (ID: %v). Cannot increase counter...", m.id)
		return
	}
	now := time.Now()
	if m.time != now.Unix() {
		if m.counter == 0 {
			m.average = 0.
		} else {
			m.average = float64(m.sum / m.counter)
		}
		m.averageN = m.counter
		m.counter = 0
		m.sum = 0
		m.averageRead = false
		m.time = now.Unix()
	}
	m.sum += (now.UnixNano() / int64(time.Millisecond)) - t
	m.counter++
}

func (m *Metric) Average() (bool, Average) {
	m.Lock()
	defer m.Unlock()
	if !m.isDone && m.averageRead {
		return false, Average{Id: m.id, Average: m.average, N: m.averageN}
	}
	if m.isDone && m.averageRead && !m.counterRead {
		m.counterRead = true
		if m.counter == 0 {
			return true, Average{Id: m.id, Average: 0., N: m.counter}
		}
		return true, Average{Id: m.id, Average: float64(m.sum / m.counter), N: m.counter}
	}
	if m.isDone && m.averageRead && m.counterRead {
		return false, Average{Id: m.id}
	}
	m.averageRead = true
	return true, Average{Id: m.id, Average: m.average, N: m.averageN}
}

func (m *Metric) setDone() {
	m.Lock()
	defer m.Unlock()
	m.isDone = true
}

func (m *Metric) Reset() {
	m.Lock()
	defer m.Unlock()
	m.time = time.Now().Unix()
	m.average = 0
	m.averageN = 0
	m.averageRead = false
	m.counter = 0
	m.counterRead = false
	m.sum = 0
	m.isDone = false
}

func (m *Metric) GetIsDone() bool {
	m.RLock()
	defer m.RUnlock()
	return m.isDone
}
