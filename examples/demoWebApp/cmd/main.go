package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-app-sdk/pkg/defra"
	"github.com/shinzonetwork/shinzo-app-sdk/pkg/networking"
	"github.com/shinzonetwork/demo-web-app/views"
	"github.com/sourcenetwork/defradb/node"
)

type Stats struct {
	UnfilteredCount int       `json:"unfilteredCount"`
	FilteredCount   int       `json:"filteredCount"`
	StartTime       time.Time `json:"startTime"`
	LastUpdate      time.Time `json:"lastUpdate"`
}

type DashboardData struct {
	UnfilteredLogs     []ViewQueryResult `json:"unfilteredLogs"`
	FilteredLogs       []ViewQueryResult `json:"filteredLogs"`
	UnfilteredViewName string            `json:"unfilteredViewName"`
	FilteredViewName   string            `json:"filteredViewName"`
	Stats              Stats             `json:"stats"`
	Uptime             string            `json:"uptime"`
	LastUpdateAgo      string            `json:"lastUpdateAgo"`
}

type ViewQueryResult struct {
	TransactionHash string `json:"transactionHash"`
}

var (
	defraNode *node.Node
	stats     = Stats{
		StartTime:  time.Now(),
		LastUpdate: time.Now(),
	}
	statsMutex sync.RWMutex
)

func main() {
	// Initialize DefraDB
	cfg := defra.DefaultConfig
	ipAddress, err := networking.GetLANIP()
	if err != nil {
		log.Fatal(err)
	}
	listenAddress := fmt.Sprintf("/ip4/%s/tcp/0", ipAddress)
	defraUrl := fmt.Sprintf("%s:0", ipAddress)
	cfg.DefraDB.Store.Path = "./.defra"
	cfg.DefraDB.Url = defraUrl
	cfg.DefraDB.P2P.ListenAddr = listenAddress
	cfg.DefraDB.P2P.BootstrapPeers = append(defra.DefaultConfig.DefraDB.P2P.BootstrapPeers, "/ip4/192.168.4.22/tcp/9176/p2p/12D3KooWLttXvtbokAphdVWL6hx7VEviDnHYwQs5SmAw1Y1yfcZT")
	cfg.Logger.Development = false

	log.Println("🚀 Starting Shinzo Web Demo App...")
	log.Println("⏳ Starting DefraDB instance...")

	defraNode, err = defra.StartDefraInstance(cfg, &defra.MockSchemaApplierThatSucceeds{})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("✓ DefraDB started successfully!")
	log.Println("⏳ Subscribing to views...")

	for _, view := range views.Views {
		err := view.SubscribeTo(context.Background(), defraNode)
		if err != nil {
			if strings.Contains(err.Error(), "collection already exists") {
				continue
			}
			log.Fatal(err)
		}
	}

	log.Printf("✓ Subscribed to %d views!\n", len(views.Views))

	// Setup HTTP routes
	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/api/data", serveData)
	http.HandleFunc("/api/stream", serveSSE)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("static"))))

	port := ":8080"
	log.Printf("🌐 Web server starting on http://localhost%s\n", port)
	log.Printf("📊 Open your browser to view the dashboard!\n")

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal(err)
	}
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	http.ServeFile(w, r, "static/index.html")
}

func serveData(w http.ResponseWriter, r *http.Request) {
	data := fetchDashboardData()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func serveSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send updates every 3 seconds
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	ctx := r.Context()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			data := fetchDashboardData()
			jsonData, err := json.Marshal(data)
			if err != nil {
				continue
			}

			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	}
}

func fetchDashboardData() DashboardData {
	ctx := context.Background()

	// Query unfiltered view
	unfilteredQuery := fmt.Sprintf(`%s {
		transactionHash
	}`, views.Views[0].Name)

	unfilteredLogs, err := defra.QueryArray[ViewQueryResult](ctx, defraNode, unfilteredQuery)
	if err != nil {
		unfilteredLogs = []ViewQueryResult{}
	}

	// Query filtered view
	filteredQuery := fmt.Sprintf(`%s {
		transactionHash
	}`, views.Views[1].Name)

	filteredLogs, err := defra.QueryArray[ViewQueryResult](ctx, defraNode, filteredQuery)
	if err != nil {
		filteredLogs = []ViewQueryResult{}
	}

	// Update stats and track if counts changed
	statsMutex.Lock()
	unfilteredCountChanged := len(unfilteredLogs) != stats.UnfilteredCount
	filteredCountChanged := len(filteredLogs) != stats.FilteredCount

	stats.UnfilteredCount = len(unfilteredLogs)
	stats.FilteredCount = len(filteredLogs)

	// Only update LastUpdate if counts actually changed
	if unfilteredCountChanged || filteredCountChanged {
		stats.LastUpdate = time.Now()
	}
	statsMutex.Unlock()

	// Calculate metrics
	uptime := time.Since(stats.StartTime)
	lastUpdateAgo := time.Since(stats.LastUpdate)

	return DashboardData{
		UnfilteredLogs:     unfilteredLogs,
		FilteredLogs:       filteredLogs,
		UnfilteredViewName: views.Views[0].Name,
		FilteredViewName:   views.Views[1].Name,
		Stats:              stats,
		Uptime:             formatDuration(uptime),
		LastUpdateAgo:      formatDuration(lastUpdateAgo) + " ago",
	}
}

func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%ds", seconds)
}
