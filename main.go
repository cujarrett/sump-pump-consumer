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
	lastRunWatts     prometheus.Gauge

	// runningAt is set when a running event is received; cleared on idle.
	// Used by the watchdog to detect stuck-running state.
	runningAt       time.Time
	lastRunWattsVal float64
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
		a.lastRunWatts.Set(p.Watts)
		a.lastRunWattsVal = p.Watts
		a.runsTotal.Inc()
		a.runningAt = time.Now()
	case "home.appliance.sump-pump.idle":
		log.Printf("sump pump idle: %.1fW", p.Watts)
		if !a.runningAt.IsZero() {
			duration := time.Since(a.runningAt)
			costUSD := a.lastRunWattsVal / 1000 * 0.16 * duration.Seconds() / 3600
			log.Printf("run_complete start=%q duration_s=%.0f watts=%.1f cost_usd=%.6f",
				a.runningAt.UTC().Format(time.RFC3339),
				duration.Seconds(),
				a.lastRunWattsVal,
				costUSD,
			)
		}
		a.running.Set(0)
		a.watts.Set(0)
		a.lastRunTimestamp.SetToCurrentTime()
		a.runningAt = time.Time{}
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
	log.Printf("404 %s %s from %s %q", r.Method, r.URL.Path, r.RemoteAddr, r.Header.Get("User-Agent"))
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
		Name: "sump_pump_last_completed_run_timestamp_unix",
		Help: "Unix timestamp when the most recent sump pump run completed.",
	})
	lastRunWatts := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "sump_pump_last_run_watts",
		Help: "Power draw in watts of the most recent sump pump run. Latches until next run.",
	})

	reg.MustRegister(msgsProcessed, runsTotal, running, watts, lastRunTimestamp, lastRunWatts)

	// Initialize to now so restarts don't show "56 years since last run".
	lastRunTimestamp.SetToCurrentTime()

	a := &app{
		msgsProcessed:    msgsProcessed,
		runsTotal:        runsTotal,
		running:          running,
		watts:            watts,
		lastRunTimestamp: lastRunTimestamp,
		lastRunWatts:     lastRunWatts,
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

	// Watchdog: if the pump has been in running state for more than 10 minutes
	// with no new event (missed idle webhook), auto-reset to idle.
	const watchdogTimeout = 10 * time.Minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if !a.runningAt.IsZero() && time.Since(a.runningAt) > watchdogTimeout {
				log.Printf("watchdog: no idle event after %v, resetting to idle", watchdogTimeout)
				duration := time.Since(a.runningAt)
				costUSD := a.lastRunWattsVal / 1000 * 0.16 * duration.Seconds() / 3600
				log.Printf("run_complete start=%q duration_s=%.0f watts=%.1f cost_usd=%.6f",
					a.runningAt.UTC().Format(time.RFC3339),
					duration.Seconds(),
					a.lastRunWattsVal,
					costUSD,
				)
				a.running.Set(0)
				a.watts.Set(0)
				a.lastRunTimestamp.SetToCurrentTime()
				a.runningAt = time.Time{}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
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
