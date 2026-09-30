// Package consumer se suscribe a onix.raw.* en NATS/JetStream y entrega cada RawEvent al store.
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/nats-io/nats.go"
	c "github.com/levapo97-cell/onix-contracts/go/onixcontracts"
)

// Recorder es lo que el consumer necesita del store (facilita tests).
type Recorder interface {
	RecordEvent(ctx context.Context, e c.RawEvent) error
}

const (
	streamName   = "ONIX_RAW"
	subjects     = "onix.raw.>"
	durableName  = "onix-core"
)

type Consumer struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

// Start conecta a NATS, garantiza el stream ONIX_RAW y arranca un consumidor durable.
func Start(ctx context.Context, natsURL string, rec Recorder) (*Consumer, error) {
	nc, err := nats.Connect(natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.Name("onix-core"),
	)
	if err != nil {
		return nil, fmt.Errorf("conectar NATS: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("jetstream: %w", err)
	}

	// Garantiza el stream (idempotente).
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subjects},
		Storage:  nats.FileStorage,
	}); err != nil && err != nats.ErrStreamNameAlreadyInUse {
		// AddStream sobre uno existente con misma config no falla; si cambia config podría.
		slog.Warn("AddStream", "err", err)
	}

	handler := func(m *nats.Msg) {
		var e c.RawEvent
		if err := json.Unmarshal(m.Data, &e); err != nil {
			slog.Error("evento raw inválido, descartado", "err", err, "subject", m.Subject)
			_ = m.Ack() // no reintentar un mensaje que nunca parseará
			return
		}
		if err := rec.RecordEvent(ctx, e); err != nil {
			slog.Error("no se pudo persistir el evento", "err", err, "session", e.Session)
			_ = m.Nak() // reintentar
			return
		}
		_ = m.Ack()
		slog.Info("evento persistido", "session", e.Session, "role", e.AgentRole, "hook", e.Hook, "tool", derefStr(e.Tool))
	}

	sub, err := js.Subscribe(subjects, handler,
		nats.Durable(durableName),
		nats.ManualAck(),
		nats.DeliverAll(),
		nats.AckExplicit(),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("suscribir: %w", err)
	}

	slog.Info("consumidor NATS activo", "stream", streamName, "subjects", subjects, "durable", durableName)
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

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
