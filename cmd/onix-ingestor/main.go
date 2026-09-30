// Command onix-ingestor — servicio "core" de OnixGuard.
//
// FASE 1: combina ingestor + recorder. Consume onix.raw.* de NATS/JetStream y persiste en
// Postgres (auto-registrando project/agent/session). Mantiene /healthz y /readyz.
// Si no hay DATABASE_URL, corre en modo health-only (útil para el smoke test de Fase 0).
//
// FASE 2 lo dividirá en onix-ingestor (valida/normaliza) y onix-recorder (persiste).
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
	"github.com/levapo97-cell/onix-ingestor/internal/store"
)

// version se rellena en tiempo de compilación con -ldflags (ver Dockerfile).
var version = "dev"

// ready refleja si el pipeline (NATS+Postgres) está operativo.
var ready atomic.Bool

func main() {
	// -healthcheck: modo cliente que usa el HEALTHCHECK de Docker.
	// Hace GET a /healthz y sale 0 (sano) o 1 (caído). No necesita shell ni curl,
	// así funciona con la imagen distroless.
	hc := flag.Bool("healthcheck", false, "hace ping a /healthz y termina (para Docker HEALTHCHECK)")
	flag.Parse()
	if *hc {
		os.Exit(runHealthcheck(env("PORT", "8081")))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg := loadConfig()
	slog.Info("onix-ingestor arrancando",
		"version", version,
		"port", cfg.Port,
		"nats_url", cfg.NatsURL,
		"has_database_url", cfg.DatabaseURL != "",
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Servidor HTTP (health) primero, para que el healthcheck responda mientras el pipeline conecta.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz)
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// Pipeline NATS→Postgres. Sin DATABASE_URL, modo health-only (Fase 0).
	var st *store.Store
	var cons *consumer.Consumer
	if cfg.DatabaseURL == "" {
		slog.Warn("sin DATABASE_URL: modo health-only, no se consume NATS")
	} else {
		var err error
		st, err = store.New(ctx, cfg.DatabaseURL)
		if err != nil {
			slog.Error("no se pudo abrir Postgres", "err", err)
			os.Exit(1)
		}
		defer st.Close()
		if err := st.WaitReady(ctx, 30*time.Second); err != nil {
			slog.Error("Postgres no listo", "err", err)
			os.Exit(1)
		}
		cons, err = consumer.Start(ctx, cfg.NatsURL, st)
		if err != nil {
			slog.Error("no se pudo arrancar el consumidor NATS", "err", err)
			os.Exit(1)
		}
		defer cons.Close()
		ready.Store(true)
		slog.Info("pipeline activo: onix.raw.* → Postgres")
	}

	select {
	case err := <-errCh:
		slog.Error("servidor falló", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		slog.Info("apagando…")
		ready.Store(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("apagado forzado", "err", err)
		}
	}
}

// config es el contrato de variables de entorno del servicio.
type config struct {
	Port        string
	NatsURL     string // usado a partir de Fase 1
	DatabaseURL string // usado a partir de Fase 1
}

func loadConfig() config {
	return config{
		Port:        env("PORT", "8081"),
		NatsURL:     env("NATS_URL", "nats://nats:4222"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// healthz: liveness. Responde 200 mientras el proceso esté vivo.
func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "onix-ingestor",
		"version": version,
	})
}

// readyz: readiness. 200 cuando el pipeline NATS→Postgres está operativo (o en health-only).
func readyz(w http.ResponseWriter, _ *http.Request) {
	if ready.Load() {
		writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "pipeline": true})
		return
	}
	// En modo health-only (sin DATABASE_URL) seguimos respondiendo 200 para no romper Fase 0.
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "pipeline": false})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

// runHealthcheck hace GET a /healthz en localhost y devuelve el código de salida del proceso.
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
