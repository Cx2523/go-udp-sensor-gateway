package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	udpAddr := net.JoinHostPort(
		os.Getenv("EDGEGATEWAY_IP"),
		os.Getenv("EDGEGATEWAY_UDPPORT"),
	)

	addr, err := net.ResolveUDPAddr("udp", udpAddr)

	if err != nil {
		log.Fatal("Listen failed:", err)
	}

	conn, err := net.ListenUDP("udp", addr)

	if err != nil {
		log.Fatal("Listen failed:", err)
	}

	defer conn.Close()

	buffer := make([]byte, 1024)

	inMemoryStore := NewStore()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /alive", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "It is alive - Sensor gateway")
	})

	mux.HandleFunc("GET /{nodeId}/{metric}", func(w http.ResponseWriter, r *http.Request) {
		nodeId := r.PathValue("nodeId")
		metric := r.PathValue("metric")

		log.Printf("Received nodeId: %v", nodeId)
		log.Printf("Received metric: %v", metric)
		latestSensorReading, found := inMemoryStore.Get(nodeId + ":" + metric)

		if found != true {
			http.Error(w, "A reading for the nodeId and metric was not found", http.StatusBadRequest)
			return
		}

		age := time.Since(latestSensorReading.ReceivedAt)
		isStale := age > 30*time.Second

		latestSensorReading.IsStale = isStale

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(latestSensorReading); err != nil {
			log.Printf("Error encoding latestSensorReading: %v", err)
		}
	})

	go func() {
		log.Printf("HTTP server listening on :%s", os.Getenv("EDGEGATEWAY_HTTPPORT"))

		if err := http.ListenAndServe(":"+os.Getenv("EDGEGATEWAY_HTTPPORT"), mux); err != nil {
			log.Fatal(err)
		}
	}()

	for {
		n, clientAddress, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		rawMessage := string(buffer[:n])
		fmt.Printf("Got message from %s: %s\n", clientAddress, rawMessage)

		sensorReading, err := parseSensorReading(rawMessage)

		if err != nil {
			log.Printf("Unable to parse message: %v", err)
		} else {
			fmt.Printf("Parsed sensor reading: %+v\n", sensorReading)
		}

		_, err = conn.WriteToUDP(buffer[:n], clientAddress)
		if err != nil {
			log.Printf("Write error: %v", err)
		}

		log.Printf("SensorReading.NodeId: %v", sensorReading.NodeId)
		log.Printf("SensorReading.Metric: %v", sensorReading.Metric)
		sensorReading.ReceivedAt = time.Now()
		if sensorReading.NodeId != "" && sensorReading.Metric != "" {
			inMemoryStore.Set(sensorReading.NodeId+":"+sensorReading.Metric, sensorReading)
		}

	}

}

// Version|Node|ReadId|Sensor_Action|Metric|Value
// V1|node-178|1042|READING|temp_f|72.45
type SensorReading struct {
	Version      string    `json:"version"`
	NodeId       string    `json:"nodeId"`
	ReadId       int       `json:"readId"`
	SensorAction string    `json:"sensorAction"`
	Metric       string    `json:"metric"`
	Value        string    `json:"value"`
	ReceivedAt   time.Time `json:"receivedAt"`
	IsStale      bool      `json:"isStale"`
}

// Version|Node|ReadId|Sensor_Action|Metric|Value
// V1|node-178|1042|READING|temp_f|72.45
func parseSensorReading(rawReading string) (SensorReading, error) {
	fields := strings.Split(rawReading, "|")
	if len(fields) != 6 {
		return SensorReading{}, fmt.Errorf(
			"invalid message format: expected 6 fields, got %d",
			len(fields),
		)
	}

	readId, err := strconv.Atoi(fields[2])

	if err != nil {
		return SensorReading{}, fmt.Errorf(
			"invalid readId %q: %w", fields[2], err,
		)
	}

	sensorReading := SensorReading{
		Version:      fields[0],
		NodeId:       fields[1],
		ReadId:       readId,
		SensorAction: fields[3],
		Metric:       fields[4],
		Value:        fields[5],
	}

	return sensorReading, nil
}

type InMemoryStore struct {
	mu   sync.RWMutex
	data map[string]SensorReading
}

func NewStore() *InMemoryStore {
	return &InMemoryStore{
		data: make(map[string]SensorReading),
	}
}

func (s *InMemoryStore) Set(key string, value SensorReading) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

func (s *InMemoryStore) Get(key string) (SensorReading, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, exists := s.data[key]
	return value, exists
}
