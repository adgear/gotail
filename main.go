// Copyright (c) 2015 Datacratic. All rights reserved.

package main

import (
	"bufio"
	"container/list"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/datacratic/gometrics"
)

type Stream struct {
	strings chan string
}

type Clients map[*Stream]*Stream

type Event struct {
	Matched   int
	Unmatched int
	Late      int
	Total     int
}

type Counts struct {
	When       time.Time
	BidRequest int
	Bid        int
	Win        Event
	Click      Event
	Conversion Event
}

type Reports struct {
	Counts list.List
	mu     sync.Mutex
}

type Tailer struct {
	Monitor *metric.Monitor
	reports Reports
	clients atomic.Value
	mu      sync.Mutex
}

type TailerMetrics struct {
	Line      bool
	Client    bool
	LinesSent int
	BytesSent int
	Counts    *Counts
}

func main() {
	address := flag.String("address", ":http", "bind address of the HTTP server")
	filename := flag.String("file", "", "file to serve")
	cpu := flag.Int("cpu", 1, "number of CPU to use")
	server := flag.String("server", "localhost", "name of the server where this component runs")
	carbon := flag.String("carbon", "", "comma separated list of TCP address of Carbon daemons")

	flag.Parse()

	if *address == "" {
		log.Fatal("missing '--address' parameter")
	}

	if *filename == "" {
		log.Fatal("missing '--file' parameter")
	}

	cmd := exec.Command("tail", "-c", "+1", "-f", *filename)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}

	trailer := new(Tailer)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			text := scanner.Text()
			trailer.process(text)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}()

	if *carbon != "" {
		urls := strings.Split(*carbon, ",")
		trailer.Monitor = metric.NewCarbonMonitor(*server, urls)
		trailer.Monitor.Name = "trail"
		trailer.Monitor.PublishInterval = 10 * time.Second
		trailer.Monitor.Start()
	}

	log.Printf("%s.trail is using %d CPU(s)\n", *server, *cpu)
	runtime.GOMAXPROCS(*cpu)

	log.Fatal(http.ListenAndServe(*address, trailer))
}

func (trailer *Tailer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/counts" {
		// lock the counts since we're going to read them
		trailer.reports.mu.Lock()
		defer trailer.reports.mu.Unlock()

		counts := &trailer.reports.Counts

		// build the array
		items := make([]*Counts, 0, counts.Len())
		for e := counts.Front(); e != nil; e = e.Next() {
			items = append(items, e.Value.(*Counts))
		}

		data, err := json.Marshal(items)
		if err != nil {
			log.Printf("marshal failed: %s\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Write(data)
		return
	}

	if r.URL.Path == "/trail" {
		flusher, ok := w.(http.Flusher)
		if !ok {
			log.Panicf("can't support HTTP streaming '%+v'", w)
		}

		closer, ok := w.(http.CloseNotifier)
		if !ok {
			log.Panicf("can't support HTTP streaming '%+v'", w)
		}

		w.Header().Set("Content-Type", "text/plain")

		// attach the stream and detach it when we're done
		stream := trailer.New()
		defer trailer.Close(stream)

		// buffer the writes
		writer := bufio.NewWriter(w)
		lines := 0
		count := 0
		total := 0

		for done := false; !done; {
			select {
			case text := <-stream.strings:
				n, err := writer.WriteString(text + "\n")
				if err != nil {
					log.Println(err)
					done = true
				}

				lines += 1
				count += n
				total += n
			case <-time.After(time.Second):
				if err := writer.Flush(); err != nil {
					log.Println(err)
					done = true
				}

				// push the data over the wire
				flusher.Flush()

				metrics := TailerMetrics{
					Client:    true,
					LinesSent: lines,
					BytesSent: count,
				}

				if trailer.Monitor != nil {
					trailer.Monitor.RecordMetrics("trailer", &metrics)
				}

				lines = 0
				count = 0
			case <-closer.CloseNotify():
				done = true
			}
		}

		log.Printf("client received '%d' bytes\n", total)
		return
	}

	log.Printf("failed to get resouce '%s'\n", r.URL.Path)
	w.WriteHeader(http.StatusNotFound)
}

func (trailer *Tailer) process(text string) {
	metrics := TailerMetrics{
		Line: true,
	}

	now := time.Now().Round(time.Hour)

	// lock the counts since we're going to update them
	trailer.reports.mu.Lock()
	defer trailer.reports.mu.Unlock()

	counts := &trailer.reports.Counts

	front := counts.Front()
	if front == nil || front.Value.(*Counts).When != now {
		front = counts.PushFront(&Counts{
			When: now,
		})
	}

	values := front.Value.(*Counts)

	// parse the text line
	offset := 16
	if len(text) > offset {
		tokens := strings.SplitAfterN(text[16:], " ", 4)
		switch tokens[0] {
		case "AUCTION":
			values.BidRequest++
		case "SUBMITTED":
			values.Bid++
		case "WIN":
			values.Win.Total++
		case "MATCHEDLOSS":
			values.Win.Late++
		case "MATCHEDWIN":
			values.Win.Matched++
		case "UNMATCHEDWIN":
			values.Win.Unmatched++
		case "CLICK":
			values.Click.Total++
		case "MATCHEDCLICK":
			values.Click.Matched++
		case "UNMATCHEDCLICK":
			values.Click.Unmatched++
		case "CONVERSION":
			values.Conversion.Total++
		case "MATCHEDCONVERSION":
			values.Conversion.Matched++
		case "UNMATCHEDCONVERSION":
			values.Conversion.Unmatched++
		}
	}

	if e := front.Next(); e != nil {
		metrics.Counts = e.Value.(*Counts)
	}

	// only keep the last month of counts
	for counts.Len() > 720 {
		counts.Remove(counts.Back())
	}

	// send text to clients currently attached
	if clients := trailer.clients.Load(); clients != nil {
		for client := range clients.(Clients) {
			client.strings <- text
		}
	}

	if trailer.Monitor != nil {
		trailer.Monitor.RecordMetrics("process", &metrics)
	}
}

func (trailer *Tailer) New() (stream *Stream) {
	stream = &Stream{
		strings: make(chan string),
	}

	trailer.mu.Lock()
	defer trailer.mu.Unlock()

	last := trailer.clients.Load()
	next := make(Clients)
	next[stream] = stream
	if last != nil {
		clients := last.(Clients)
		for item := range clients {
			next[item] = item
		}
	}

	trailer.clients.Store(next)
	return
}

func (trailer *Tailer) Close(stream *Stream) {
	trailer.mu.Lock()
	defer trailer.mu.Unlock()

	clients := trailer.clients.Load().(Clients)
	next := make(Clients)
	for item := range clients {
		if item == stream {
			continue
		}

		next[item] = item
	}

	trailer.clients.Store(next)
}
