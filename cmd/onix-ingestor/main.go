// Command onix-ingestor.
//
// FASE 0 (Cimientos): este binario es el "servicio core temporal". Solo expone /healthz
// y /readyz para validar que el docker-compose levanta y que el edge puede hacer health-check.
//
// FASE 1 crecerá este servicio hasta consumir onix.raw.* de NATS, validar/normalizar y
// (combinado con recorder) persistir en Postgres. Por eso ya lee NATS_URL y DATABASE_URL del
// entorno aunque todavía no los use: deja el contrato de configuración fijado.
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
	"syscall"
	"time"
)

// buildInfo se rellena en tiempo de compilación con -ldflags (ver Dockerfile).
var version = "dev"

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

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz)

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Arranque + apagado ordenado (SIGINT/SIGTERM).
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		slog.Error("servidor falló", "err", err)
		os.Exit(1)
	case <-ctx.Done():
		slog.Info("apagando…")
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

// readyz: readiness. En Fase 0 equivale a healthz; en Fase 1 comprobará NATS/Postgres.
func readyz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
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
