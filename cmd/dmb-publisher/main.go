package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	client "github.com/Takahiro55555/location-based-mqtt-client.golang"
	"github.com/golang/geo/s2"
)

type Message struct {
	Id      string `json:"id"`
	TimeMs  uint64 `json:"time_ms"`
	Padding string `json:"padding"`
}

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Print("Starting...")
	host := flag.String("host", "127.0.0.1", "ブローカーホスト名")
	port := flag.Int("port", 1883, "ブローカーポート番号")
	pid := flag.String("pid", "", "Subscriber 側がプロセスを識別するための ID")
	msglen := flag.Int("msglen", 100, "送信するメッセージの文字数")
	clientNum := flag.Int("clients", 100, "クライアント数")
	rutines := flag.Int("rutines", 100, "Gorutine の数")
	t := flag.Int("time", 100, "計測時間[sec]")
	interval := flag.Int("interval", 100, "Publish した後に sleep する時間[ms]")
	prefix := flag.String("prefix", "/0", "Publish する際のトピック名の接頭辞")
	seed := flag.Int64("seed", time.Now().UnixNano(), "pid や padding を生成するためのシード値")
	flag.Parse()
	rand.Seed(*seed)

	if *pid == "" {
		*pid = randString1(10)
	}

	// オプションの表示
	log.Printf("OPTION Manager broker hostname    : %v", *host)
	log.Printf("OPTION Manager broker port        : %v", *port)
	log.Printf("OPTION Message length             : %v", *msglen)
	log.Printf("OPTION Client num                 : %v", *clientNum)
	log.Printf("OPTION Gorutine num               : %v", *rutines)
	log.Printf("OPTION Measurement time           : %v [s]", *t)
	log.Printf("OPTION Publish interval           : %v [ms]", *interval)
	log.Printf("OPTION Process id                 : %v", *pid)
	log.Printf("OPTION Process prefix             : %v", *prefix)
	log.Printf("OPTION Seed                       : %v", *seed)

	// 送信メッセージの生成
	padding := randString1(*msglen)

	clients := make([]*client.Client, *clientNum)
	log.Print("Allocated!!!")
	now := time.Now().UnixNano()
	topic := int2Topicname(uint64(now), 32, *prefix)
	log.Print(topic)
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
	metrics := NewMetrics()
	defer func() {
		metrics.SetIsDone()
		for i := 0; i < 5; i++ {
			ok, rate := metrics.GetRate()
			if ok {
				log.Printf("Publish rate: %v [pub/s]", rate)
			}
			time.Sleep(time.Millisecond * 50)
		}
		// msg := fmt.Sprintf("{\"id\":\"%v\",\"time_ms\":%v,\"is_done\":true}", *pid, (time.Now().UnixNano() / int64(time.Millisecond)))
		// if token := clients[0].Publish("/signal", 1, false, msg); token.Wait() && token.Error() != nil {
		// 	log.Printf("MQTT Publish error (done signal): %s", token.Error())
		// }
		for i, c := range clients {
			c.Disconnect(500)
			log.Printf("Disconnecting... (%v)", i)
		}
	}()

	doneCh := make(chan bool)
	go func() {
		for st := time.Now().Unix(); time.Now().Unix()-st < int64(*t); {
			ok, rate := metrics.GetRate()
			if ok {
				log.Printf("Publish rate: %v [pub/s]", rate)
			}
			time.Sleep(time.Millisecond * 300)
		}
		metrics.SetIsDone()
		for i := 0; i < 5; i++ {
			ok, rate := metrics.GetRate()
			if ok {
				log.Printf("Publish rate: %v [pub/s]", rate)
			}
			time.Sleep(time.Millisecond * 500)
		}
		doneCh <- true
	}()

	msg := Message{Id: *pid, TimeMs: uint64(now / int64(time.Millisecond)), Padding: ""}
	payload, err := json.Marshal(msg)
	if err != nil {
		log.Fatalf("JSON encoding error (pub), %v", err)
	}
	fmt.Println(string(payload))
	if *msglen-len(string(payload)) > 0 {
		padding = padding[:(*msglen - len(string(payload)))]
	} else {
		padding = ""
	}

	log.Print("Starting goroutine...")
	time.Sleep(time.Millisecond * 100)
	for i := 0; i < *rutines; i++ {
		go pub(clients[i%*clientNum], metrics, time.Duration(*interval), *msglen, *pid, padding, *prefix)
	}
	log.Print("Done launching goroutine.")
	time.Sleep(time.Second)

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

func pub(c *client.Client, metrics *Metrics, interval time.Duration, msgLen int, pid, padding, prefix string) {

	for i := 0; true; i++ {
		now := time.Now().UnixNano()
		t := int2Topicname(uint64(now), 32, prefix)
		token, err := TopicName2Token(t)
		if err != nil {
			log.Fatalf("Topic name translation error (pub), %s", err)
		}
		latlng := s2.LatLngFromPoint(s2.CellIDFromToken(token).Point())
		if metrics.GetIsDone() {
			break
		}

		msg := Message{Id: pid, TimeMs: uint64(now / int64(time.Millisecond)), Padding: padding}
		payload, err := json.Marshal(msg)
		if err != nil {
			log.Fatalf("JSON encoding error (pub), %v", err)
		}

		if err := c.Publish(latlng.Lat.Degrees(), latlng.Lng.Degrees(), 0, false, string(payload)); err != nil {
			log.Fatalf("Mqtt error: %s", err)
		}

		metrics.Countup()
		time.Sleep(time.Millisecond * interval)
	}
}

func randString1(n int) string {
	rs1Letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = rs1Letters[rand.Intn(len(rs1Letters))]
	}
	return string(b)
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

type Metrics struct {
	sync.RWMutex
	time        int64
	rate        uint64
	rateRead    bool
	counter     uint64
	counterRead bool
	isDone      bool
}

func NewMetrics() *Metrics {
	return &Metrics{time: time.Now().Unix(), rate: 0, counter: 0, isDone: false, rateRead: false, counterRead: false}
}

func (m *Metrics) Countup() {
	m.Lock()
	defer m.Unlock()
	now := time.Now().Unix()
	if m.time != now {
		m.rate = m.counter
		m.counter = 0
		m.time = now
		m.rateRead = false
	}
	m.counter++
}

func (m *Metrics) GetIsDone() bool {
	m.RLock()
	defer m.RUnlock()
	return m.isDone
}

func (m *Metrics) SetIsDone() {
	m.Lock()
	defer m.Unlock()
	m.isDone = true
}

func (m *Metrics) GetRate() (bool, uint64) {
	m.Lock()
	defer m.Unlock()
	if !m.isDone && m.rateRead {
		return false, m.rate
	}
	if m.isDone && m.rateRead && !m.counterRead {
		m.counterRead = true
		return true, m.counter
	}
	if m.isDone && m.rateRead && m.counterRead {
		return false, 0
	}
	m.rateRead = true
	return true, m.rate
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
