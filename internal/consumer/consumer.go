// Package consumer: FASE 2. Consume onix.raw.*, valida y NORMALIZA el evento, y publica
// onix.norm.*. Ya NO persiste (eso es onix-recorder). No analiza ni redacta (eso es onix-guard).
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go"
	c "github.com/levapo97-cell/onix-contracts/go/onixcontracts"
)

const (
	streamRaw  = "ONIX_RAW"
	subjRawAll = "onix.raw.>"
	streamNorm = "ONIX_NORM"
	subjNorm   = "onix.norm."
	subjNormAll = "onix.norm.>"
	durable    = "onix-ingestor"
)

type Consumer struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

// Start conecta a NATS, garantiza los streams ONIX_RAW (consume) y ONIX_NORM (publica),
// y arranca un consumidor durable que normaliza raw → norm.
func Start(ctx context.Context, natsURL string) (*Consumer, error) {
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.Name("onix-ingestor"))
	if err != nil {
		return nil, fmt.Errorf("conectar NATS: %w", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}
	ensureStream(js, streamRaw, subjRawAll)
	ensureStream(js, streamNorm, subjNormAll)

	handler := func(m *nats.Msg) {
		var raw c.RawEvent
		if err := json.Unmarshal(m.Data, &raw); err != nil {
			slog.Error("raw inválido, descartado", "err", err)
			_ = m.Ack()
			return
		}
		if !valid(raw) {
			slog.Warn("raw incompleto, descartado", "session", raw.Session)
			_ = m.Ack()
			return
		}
		norm := normalize(raw)
		data, _ := json.Marshal(norm)
		if _, err := js.Publish(subjNorm+raw.Session, data); err != nil {
			slog.Error("no se pudo publicar norm", "err", err, "session", raw.Session)
			_ = m.Nak()
			return
		}
		_ = m.Ack()
		slog.Info("normalizado → norm", "session", norm.Session, "hook", norm.Hook, "tool", deref(norm.Tool))
	}

	sub, err := js.Subscribe(subjRawAll, handler,
		nats.Durable(durable), nats.ManualAck(), nats.DeliverAll(), nats.AckExplicit())
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("suscribir raw: %w", err)
	}
	slog.Info("ingestor activo", "consume", subjRawAll, "publica", subjNormAll)
	return &Consumer{nc: nc, sub: sub}, nil
}

func (c *Consumer) Close() {
	if c.sub != nil {
		_ = c.sub.Drain()
	}
	if c.nc != nil {
		c.nc.Drain()
	}
}

// valid comprueba los campos obligatorios del contrato raw.
func valid(e c.RawEvent) bool {
	return e.Session != "" && e.Project != "" && string(e.AgentRole) != "" && string(e.Hook) != ""
}

// normalize convierte un RawEvent en NormEvent: misma info + received_at (UTC).
func normalize(r c.RawEvent) c.NormEvent {
	n := c.NormEvent{
		V:          1,
		Session:    r.Session,
		AgentRole:  r.AgentRole,
		Hook:       r.Hook,
		Ts:         r.Ts.UTC(),
		ReceivedAt: time.Now().UTC(),
		Tool:       r.Tool,
		Params:     r.Params,
		Tokens:     r.Tokens,
		CostUsd:    r.CostUsd,
		Cwd:        r.Cwd,
		Project:    r.Project,
		Stage:      r.Stage,
	}
	if r.Result != nil {
		n.Result = &c.NormEventResult{ExitCode: r.Result.ExitCode, DurationMS: r.Result.DurationMS}
	}
	return n
}

func ensureStream(js nats.JetStreamContext, name, subjects string) {
	if _, err := js.AddStream(&nats.StreamConfig{
		Name: name, Subjects: []string{subjects}, Storage: nats.FileStorage,
	}); err != nil && err != nats.ErrStreamNameAlreadyInUse {
		slog.Warn("AddStream", "stream", name, "err", err)
	}
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
