package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// version is set at build time via -ldflags="-X main.version=x.y.z".
var version = "dev"

// natsPayload is the JSON body published by sump-pump-bridge.
type natsPayload struct {
	Watts float64 `json:"watts"`
}

// app holds all dependencies. Config is read once in main(); methods are on *app.
type app struct {
	msgsProcessed    prometheus.Counter
	runsTotal        prometheus.Counter
	running          prometheus.Gauge
	watts            prometheus.Gauge
	lastRunTimestamp prometheus.Gauge
}

// handleMessage is called by the JetStream consumer goroutine for each message.
func (a *app) handleMessage(msg jetstream.Msg) {
	a.msgsProcessed.Inc()

	var p natsPayload
	if err := json.Unmarshal(msg.Data(), &p); err != nil {
		log.Printf("unmarshal %q: %v", msg.Subject(), err)
	}

	switch msg.Subject() {
	case "home.appliance.sump-pump.running":
		log.Printf("sump pump running: %.1fW", p.Watts)
		a.running.Set(1)
		a.watts.Set(p.Watts)
		a.runsTotal.Inc()
		a.lastRunTimestamp.SetToCurrentTime()
	case "home.appliance.sump-pump.idle":
		log.Printf("sump pump idle: %.1fW", p.Watts)
		a.running.Set(0)
		a.watts.Set(0)
	default:
		log.Printf("unhandled subject %q", msg.Subject())
	}

	if err := msg.Ack(); err != nil {
		log.Printf("ack error: %v", err)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"ok","version":%q}`, version) //nolint:errcheck
}

func notFoundHandler(w http.ResponseWriter, r *http.Request) {
	log.Printf("404 %s %s %q", r.Method, r.URL.Path, r.Header.Get("User-Agent"))
	w.WriteHeader(http.StatusNotFound)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	// NATS_CONSUMER is injected by the XApi composition when subscriptionRef is set.
	// The value matches the XSubscription metadata.name (= the NATS durable consumer name).
	consumerName := os.Getenv("NATS_CONSUMER")
	if consumerName == "" {
		consumerName = "sump-pump-monitor"
	}
	// Stream name is derived by the XSubscription composition as topicRef.name | upper.
	// Override with NATS_STREAM if the stream is ever renamed.
	stream := "HOME-APPLIANCES"
	if s := os.Getenv("NATS_STREAM"); s != "" {
		stream = s
	}

	reg := prometheus.NewRegistry()

	msgsProcessed := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "messages_processed_total",
		Help: "Total NATS messages processed.",
	})
	runsTotal := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "sump_pump_runs_total",
		Help: "Total number of sump pump run cycles observed.",
	})
	running := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sump_pump_running",
		Help: "1 if the sump pump is currently running, 0 if idle.",
	})
	watts := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sump_pump_watts",
		Help: "Last reported power draw of the sump pump in watts.",
	})
	lastRunTimestamp := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sump_pump_last_run_timestamp_unix",
		Help: "Unix timestamp of the most recent sump pump run start.",
	})

	reg.MustRegister(msgsProcessed, runsTotal, running, watts, lastRunTimestamp)

	a := &app{
		msgsProcessed:    msgsProcessed,
		runsTotal:        runsTotal,
		running:          running,
		watts:            watts,
		lastRunTimestamp: lastRunTimestamp,
	}

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("nats connect %s: %v", natsURL, err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream init: %v", err)
	}

	consumer, err := js.Consumer(context.Background(), stream, consumerName)
	if err != nil {
		log.Fatalf("bind consumer %s/%s: %v", stream, consumerName, err)
	}
	cc, err := consumer.Consume(a.handleMessage)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}
	defer cc.Stop()

	log.Printf("sump-pump-consumer %s consuming %s/%s", version, stream, consumerName)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/", notFoundHandler)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{
		Addr:              ":" + metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("metrics listening on :%s", metricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		log.Printf("sump-pump-consumer %s listening on :%s", version, port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// Block until SIGINT or SIGTERM, then stop the consumer cleanly.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down")
}

