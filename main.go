package main

import (
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const dbTimeLayout = "2006-01-02T15:04:05.000000000Z"

//go:embed templates/*
var templateFiles embed.FS

func main() {
	db, err := initDatabase("./gateway.db")
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()

	log.Println("Database initialized successfully")

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

	// inMemoryStore := NewStore()

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
		// latestSensorReading, found := inMemoryStore.Get(nodeId + ":" + metric)

		// if found != true {
		// 	http.Error(w, "A reading for the nodeId and metric was not found", http.StatusBadRequest)
		// 	return
		// }

		latestSensorReading, err := getLastSensorReading(db, nodeId, metric)

		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				http.Error(w, "Reading not found", http.StatusNotFound)
			} else {
				log.Printf("Database error: %v", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}
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

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		allSensorReading, err := getAllSensorReadings(db)

		if err != nil {
			log.Printf("Error getting latestSensorReading from database: %v", err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(allSensorReading); err != nil {
			log.Printf("Error encoding allSensorReading: %v", err)
		}
	})

	tmpl := template.Must(
		template.ParseFS(templateFiles, "templates/dashboard.html"),
	)

	mux.HandleFunc("GET /dashboard", func(w http.ResponseWriter, r *http.Request) {
		// Example data; eventually query SQLite.
		data := DashboardData{
			NodeID:      "node-177",
			Temperature: 72.5,
			Humidity:    45.2,
		}

		if err := tmpl.Execute(w, data); err != nil {
			log.Printf("render dashboard: %v", err)
		}
	})

	edgeGatewayIngestEnabled, err := strconv.ParseBool(os.Getenv("EDGEGATEWAY_INGEST_ENABLED"))

	if err != nil {
		log.Printf("EDGEGATEWAY_INGEST_ENABLED could not be parsed and so is disabled: %v", err)
		edgeGatewayIngestEnabled = false
	}

	log.Printf("EDGEGATEWAY_INGEST_ENABLED: %v", edgeGatewayIngestEnabled)

	if edgeGatewayIngestEnabled {
		go func() {
			buffer := make([]byte, 1024)

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
					if err := insertSensorReading(db, sensorReading); err != nil {
						log.Printf("Failed to save sensor reading: %v", err)
					}
					// inMemoryStore.Set(sensorReading.NodeId+":"+sensorReading.Metric, sensorReading)
				}
			}
		}()
	}

	log.Printf("HTTP server listening on :%s", os.Getenv("EDGEGATEWAY_HTTPPORT"))

	if err := http.ListenAndServe(":"+os.Getenv("EDGEGATEWAY_HTTPPORT"), mux); err != nil {
		log.Fatal(err)
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

func initDatabase(dbPath string) (*sql.DB, error) {
	// Open existing database or create it if it doesn't exist.
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}

	// Create SensorReadings table.
	createTable := `
        CREATE TABLE IF NOT EXISTS SensorReadings (
            Id           INTEGER PRIMARY KEY,
            Version      TEXT NOT NULL,
            NodeId       TEXT NOT NULL,
            ReadId       INTEGER NOT NULL,
            SensorAction TEXT NOT NULL,
            Metric       TEXT NOT NULL,
            Value        TEXT NOT NULL,
            ReceivedAt   TEXT NOT NULL
        );
    `

	if _, err := db.Exec(createTable); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating SensorReadings table: %w", err)
	}

	// Create index for querying latest readings by node and metric.
	createIndex := `
        CREATE INDEX IF NOT EXISTS IX_SensorReadings_NodeId_Metric_ReceivedAt
        ON SensorReadings (NodeId, Metric, ReceivedAt DESC);
    `

	if _, err := db.Exec(createIndex); err != nil {
		db.Close()
		return nil, fmt.Errorf("creating SensorReadings index: %w", err)
	}

	return db, nil
}

func insertSensorReading(db *sql.DB, reading SensorReading) error {
	query := `
        INSERT INTO SensorReadings (
            Version,
            NodeId,
            ReadId,
            SensorAction,
            Metric,
            Value,
            ReceivedAt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
    `

	_, err := db.Exec(
		query,
		reading.Version,
		reading.NodeId,
		reading.ReadId,
		reading.SensorAction,
		reading.Metric,
		reading.Value,
		reading.ReceivedAt.UTC().Format(dbTimeLayout),
	)

	if err != nil {
		return fmt.Errorf("inserting sensor reading: %w", err)
	}

	return nil
}

func getLastSensorReading(db *sql.DB, nodeId string, metric string) (SensorReading, error) {
	query := `
        SELECT Version, NodeId, ReadId, SensorAction,
               Metric, Value, ReceivedAt
        FROM SensorReadings
        WHERE NodeId = ? AND Metric = ?
        ORDER BY ReceivedAt DESC, Id DESC
        LIMIT 1;
    `

	var s SensorReading
	var receivedAt string

	err := db.QueryRow(query, nodeId, metric).Scan(
		&s.Version,
		&s.NodeId,
		&s.ReadId,
		&s.SensorAction,
		&s.Metric,
		&s.Value,
		&receivedAt,
	)

	if err != nil {
		return SensorReading{}, fmt.Errorf(
			"getting latest sensor reading: %w", err,
		)
	}

	s.ReceivedAt, err = time.Parse(time.RFC3339Nano, receivedAt)
	if err != nil {
		return SensorReading{}, fmt.Errorf(
			"parsing ReceivedAt: %w", err,
		)
	}

	return s, nil
}

func getAllSensorReadings(db *sql.DB) ([]SensorReading, error) {
	query := `
        SELECT Version, NodeId, ReadId, SensorAction,
               Metric, Value, ReceivedAt
        FROM SensorReadings
        ORDER BY ReceivedAt DESC, Id DESC;
    `

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf(
			"getting all sensor readings: %w", err,
		)
	}
	defer rows.Close()

	sensorReadings := make([]SensorReading, 0)

	for rows.Next() {
		var s SensorReading
		var receivedAt string

		err := rows.Scan(
			&s.Version,
			&s.NodeId,
			&s.ReadId,
			&s.SensorAction,
			&s.Metric,
			&s.Value,
			&receivedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scanning reading: %w", err)
		}

		s.ReceivedAt, err = time.Parse(time.RFC3339Nano, receivedAt)
		if err != nil {
			return nil, fmt.Errorf("parsing ReceivedAt: %w", err)
		}

		sensorReadings = append(sensorReadings, s)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating readings: %w", err)
	}

	return sensorReadings, nil
}

type DashboardData struct {
	NodeID      string
	Temperature float64
	Humidity    float64
}
