package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Print("Starting...")
	host := flag.String("host", "127.0.0.1", "ブローカーホスト名")
	port := flag.String("port", "1883", "ブローカーポート番号")
	pid := flag.String("pid", "", "Subscriber 側がプロセスを識別するための ID")
	msglen := flag.Int("msglen", 100, "送信するメッセージの文字数")
	clientNum := flag.Int("clients", 100, "クライアント数")
	rutines := flag.Int("rutines", 100, "Gorutine の数")
	t := flag.Int("time", 100, "計測時間[sec]")
	interval := flag.Int("interval", 100, "Publish した後に sleep する時間[ms]")
	seed := flag.Int64("seed", time.Now().UnixNano(), "pid や padding を生成するためのシード値")
	flag.Parse()
	rand.Seed(*seed)

	if *pid == "" {
		*pid = randString1(10)
	}

	// オプションの表示
	log.Printf("OPTION Broker hostname            : %v", *host)
	log.Printf("OPTION Broker port                : %v", *port)
	log.Printf("OPTION Message length             : %v", *msglen)
	log.Printf("OPTION Client num                 : %v", *clientNum)
	log.Printf("OPTION Gorutine num               : %v", *rutines)
	log.Printf("OPTION Measurement time           : %v [s]", *t)
	log.Printf("OPTION Publish interval           : %v [ms]", *interval)
	log.Printf("OPTION Process id                 : %v", *pid)
	log.Printf("OPTION Seed                       : %v", *seed)

	// 送信メッセージの生成
	padding := randString1(*msglen)

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
		msg := fmt.Sprintf("{\"id\":\"%v\",\"time_ms\":%v,\"is_done\":true}", *pid, (time.Now().UnixNano() / int64(time.Millisecond)))
		if token := clients[0].Publish("/signal", 1, false, msg); token.Wait() && token.Error() != nil {
			log.Printf("MQTT Publish error (done signal): %s", token.Error())
		}
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

	log.Print("Starting goroutine...")
	time.Sleep(time.Millisecond * 100)
	for i := 0; i < *rutines; i++ {
		go pub(clients[i%*clientNum], metrics, time.Duration(*interval), *msglen, *pid, padding)
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

func pub(c mqtt.Client, metrics *Metrics, interval time.Duration, msgLen int, pid string, padding string) {
	for i := 0; true; i++ {
		now := time.Now().UnixNano()
		t := int2Topicname(uint64(now), 32)
		if metrics.GetIsDone() {
			break
		}
		if token := c.Publish(t, 0, false, makeMessage(msgLen, pid, padding)); token.Wait() && token.Error() != nil {
			log.Printf("MQTT Publish error: %s", token.Error())
			return
		}
		metrics.Countup()
		time.Sleep(time.Millisecond * interval)
	}
}

func makeMessage(n int, id string, padding string) string {
	msg := fmt.Sprintf("{\"id\":\"%v\",\"time_ms\":%v,", id, (time.Now().UnixNano() / int64(time.Millisecond)))
	paddingLen := n - len(msg) - len("{\"padding\":\"\"}")
	if paddingLen > 0 {
		return fmt.Sprintf("%v\"padding\":\"%v\"}", msg, padding)
	}
	return fmt.Sprintf("%v\"padding\":\"\"}", msg)
}

func randString1(n int) string {
	rs1Letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	b := make([]rune, n)
	for i := range b {
		b[i] = rs1Letters[rand.Intn(len(rs1Letters))]
	}
	return string(b)
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
