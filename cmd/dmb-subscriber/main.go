package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	client "github.com/Takahiro55555/location-based-mqtt-client.golang"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/geo/s2"
)

type Message struct {
	Id      string `json:"id"`
	TimeMs  uint64 `json:"time_ms"`
	Padding string `json:"padding"`
}

func init() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func main() {
	log.Print("Starting...")
	host := flag.String("host", "127.0.0.1", "ブローカーホスト名")
	port := flag.Int("port", 1883, "ブローカーポート番号")
	clientNum := flag.Int("clients", 1, "クライアント数")
	waitSec := flag.Int("waitsec", 1, "Publisherからの終了シグナルを受信してから、実際にSubscribeを終了するまでの秒数")
	prefix := flag.String("prefix", "/0", "Publish する際のトピック名の接頭辞")
	suscRadiusKm := flag.Float64("subR", 10., "メッセージ受信半径(Km)")
	flag.Parse()

	// オプションの表示
	log.Printf("OPTION Broker hostname            : %v", *host)
	log.Printf("OPTION Broker port                : %v", *port)
	log.Printf("OPTION Client num                 : %v", *clientNum)
	log.Printf("OPTION Wait time                  : %v [sec]", *waitSec)
	log.Printf("OPTION Process prefix             : %v", *prefix)
	log.Printf("OPTION Subscribe area radius      : %v [KM]", *suscRadiusKm)

	clients := make([]*client.Client, *clientNum)
	log.Print("Allocated!!!")
	now := time.Now().UnixNano()
	topic := int2Topicname(uint64(now), 32, *prefix)
	token, err := TopicName2Token(topic)
	if err != nil {
		log.Fatalf("TopicName2Token: %s", err)
	}
	id := s2.CellIDFromToken(token)
	latlng := s2.LatLngFromPoint(id.Point())
	for i := 0; i < *clientNum; i++ {
		// ゲートウェイブローカへ接続
		c, err := client.Connect(*host, uint16(*port), latlng.Lat.Degrees(), latlng.Lng.Degrees(), 100., 1000)
		if err != nil {
			log.Fatalf("MQTT Connect error: %s", err)
		}
		log.Printf("Client counter: %v", i+1)
		clients[i] = c
	}
	defer func(clients []*client.Client) {
		for i, c := range clients {
			c.Unsubscribe()
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

		go sub(clients[i], *prefix, measurementHandler)
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

func sub(c *client.Client, prefix string, measurementHandler mqtt.MessageHandler) {
	now := time.Now().UnixNano()
	topic := int2Topicname(uint64(now), 32, prefix)
	log.Print(topic)
	token, err := TopicName2Token(topic)
	if err != nil {
		log.Fatalf("TopicName2Token: %s", err)
	}
	id := s2.CellIDFromToken(token)
	latlng := s2.LatLngFromPoint(id.Point())

	if err := c.UpdateSubscribe(latlng.Lat.Degrees(), latlng.Lng.Degrees(), 0, measurementHandler); err != nil {
		log.Fatalf("Mqtt error: %s", err)
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

func int2Topicname(t uint64, level int, prefix string) string {
	mask := uint64(0b1100000000000000000000000000000000000000000000000000000000000000)
	topic := prefix
	level -= (len(strings.Split(prefix, "/")) - 1)
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

func (ms *Metrics) SetDoneAll() {
	ms.RLock()
	defer ms.RUnlock()
	for _, m := range ms.metrics {
		m.setDone()
	}
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

func TopicName2Token(topic string) (string, error) {
	tmp := strings.Replace(topic, "/", "", -1)
	if len(tmp) == 1 {
		return "", TopicNameError{fmt.Sprintf("Invalid topic name (inputed topic name: %v)", topic)}
	}
	var token uint64
	switch string(tmp[0]) {
	case "0":
		token = 0b0000000000000000000000000000000000000000000000000000000000000000
	case "1":
		token = 0b0010000000000000000000000000000000000000000000000000000000000000
	case "2":
		token = 0b0100000000000000000000000000000000000000000000000000000000000000
	case "3":
		token = 0b0110000000000000000000000000000000000000000000000000000000000000
	case "4":
		token = 0b1000000000000000000000000000000000000000000000000000000000000000
	case "5":
		token = 0b1010000000000000000000000000000000000000000000000000000000000000
	default:
		return "", TopicNameError{fmt.Sprintf("Invalid topic name (inputed topic name: %v)", topic)}
	}
	maskTail := uint64(0b0001000000000000000000000000000000000000000000000000000000000000)
	masks := [3]uint64{
		0b0000100000000000000000000000000000000000000000000000000000000000,
		0b0001000000000000000000000000000000000000000000000000000000000000,
		0b0001100000000000000000000000000000000000000000000000000000000000,
	}
	for _, v := range tmp[1:] {
		switch string(v) {
		case "0":
			// 何もしない
		case "1":
			token = token | masks[0]
		case "2":
			token = token | masks[1]
		case "3":
			token = token | masks[2]
		default:
			return "", TopicNameError{fmt.Sprintf("Invalid topic name (inputed topic name: %v)", topic)}
		}

		for j := 0; j < 3; j++ {
			masks[j] = masks[j] >> 2
		}
		maskTail = maskTail >> 2
	}
	tokenString := uint2Token(token | maskTail)
	tokenLen := 1
	tokenLen += int((len(tmp) - 1) / 2)
	return tokenString[:tokenLen], nil
}

func uint2Token(ui uint64) string {
	token := ""
	mask := uint64(0b1111000000000000000000000000000000000000000000000000000000000000)

	for i := 0; i < 16; i++ {
		tmp := (ui & mask)
		for j := i + 1; j < 16; j++ {
			tmp = tmp >> 4
		}
		token += fmt.Sprintf("%x", tmp)
		mask = mask >> 4
	}
	return token
}

//////////////           以下、エラー 関連                 //////////////
type TopicNameError struct {
	Msg string
}

func (e TopicNameError) Error() string {
	return fmt.Sprintf("Error: %v", e.Msg)
}

//////////////           以上、エラー 関連                 //////////////
