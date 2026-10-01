// Command onix-ingestor — normalizador de OnixGuard (FASE 2).
//
// Consume onix.raw.* de NATS/JetStream, valida y normaliza el evento (añade received_at),
// y publica onix.norm.*. NO persiste (eso es onix-recorder) ni redacta/analiza (eso es onix-guard).
// Expone /healthz y /readyz.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/levapo97-cell/onix-ingestor/internal/consumer"
)

var version = "dev"
var ready atomic.Bool

func main() {
	hc := flag.Bool("healthcheck", false, "hace ping a /healthz y termina (para Docker HEALTHCHECK)")
	flag.Parse()
	if *hc {
		os.Exit(runHealthcheck(env("PORT", "8081")))
	}

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	port := env("PORT", "8081")
	natsURL := env("NATS_URL", "nats://nats:4222")
	slog.Info("onix-ingestor arrancando", "version", version, "port", port, "nats_url", natsURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz)
	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	cons, err := consumer.Start(ctx, natsURL)
	if err != nil {
		slog.Error("no se pudo arrancar el consumidor NATS", "err", err)
		os.Exit(1)
	}
	defer cons.Close()
	ready.Store(true)
	slog.Info("pipeline activo: onix.raw.* → onix.norm.*")

	select {
	case err := <-errCh:
		slog.Error("servidor falló", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		slog.Info("apagando…")
		ready.Store(false)
		sc, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(sc)
	}
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": "onix-ingestor", "version": version})
}

func readyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "pipeline": ready.Load()})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func runHealthcheck(port string) int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
