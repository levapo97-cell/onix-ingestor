// Package store escribe los eventos en PostgreSQL y auto-registra project/agent/session.
// FASE 1: inserción simple por evento (el batch real llega cuando se separe onix-recorder).
package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	c "github.com/levapo97-cell/onix-contracts/go/onixcontracts"
)

type Store struct {
	pool *pgxpool.Pool
}

// New abre un pool de conexiones a Postgres.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.MaxConns = 5
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("conectar postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// RecordEvent auto-registra project/agent/session (idempotente) e inserta el evento.
// Recibe un RawEvent (Fase 1 consume raw directo, sin onix-guard).
func (s *Store) RecordEvent(ctx context.Context, e c.RawEvent) error {
	projectID, err := s.ensureProject(ctx, e.Project)
	if err != nil {
		return fmt.Errorf("ensureProject: %w", err)
	}
	agentID, err := s.ensureAgent(ctx, projectID, string(e.AgentRole))
	if err != nil {
		return fmt.Errorf("ensureAgent: %w", err)
	}
	sessionID, err := s.ensureSession(ctx, projectID, agentID, e.Session)
	if err != nil {
		return fmt.Errorf("ensureSession: %w", err)
	}

	// SessionStart no genera un "evento" de actividad; con registrar la sesión basta.
	if string(e.Hook) == "SessionStart" {
		return nil
	}

	evType, isErr := classify(e)
	var durationMs *int64
	if e.Result != nil && e.Result.DurationMS != nil {
		durationMs = e.Result.DurationMS
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO events (session_id, project_id, agent_id, ts, type, tool, summary, duration_ms, tokens, cost_usd, is_error, is_repetition)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,false)`,
		sessionID, projectID, agentID, e.Ts, evType, nullStr(e.Tool), summary(e), durationMs, e.Tokens, e.CostUsd, isErr,
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

// classify deriva el tipo de evento y si fue error (Fase 1: reglas mínimas, sin onix-guard).
func classify(e c.RawEvent) (evType string, isErr bool) {
	if e.Result != nil && e.Result.ExitCode != nil && *e.Result.ExitCode != 0 {
		return "error", true
	}
	if e.Tool != nil && *e.Tool != "" {
		return "tool", false
	}
	return "nota", false
}

func summary(e c.RawEvent) string {
	if e.Tool != nil && *e.Tool != "" {
		return *e.Tool
	}
	return string(e.Hook)
}

func (s *Store) ensureProject(ctx context.Context, name string) (string, error) {
	if name == "" {
		name = "desconocido"
	}
	var id string
	err := s.pool.QueryRow(ctx, `SELECT id FROM projects WHERE name=$1 LIMIT 1`, name).Scan(&id)
	if err == nil {
		return id, nil
	}
	err = s.pool.QueryRow(ctx, `INSERT INTO projects (name) VALUES ($1) RETURNING id`, name).Scan(&id)
	return id, err
}

func (s *Store) ensureAgent(ctx context.Context, projectID, role string) (string, error) {
	if role == "" {
		role = "fullstack"
	}
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO agents (project_id, role) VALUES ($1,$2)
		 ON CONFLICT (project_id, role) DO UPDATE SET role=EXCLUDED.role
		 RETURNING id`, projectID, role).Scan(&id)
	return id, err
}

func (s *Store) ensureSession(ctx context.Context, projectID, agentID, claudeSessionID string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`INSERT INTO sessions (project_id, agent_id, claude_session_id) VALUES ($1,$2,$3)
		 ON CONFLICT (claude_session_id) DO UPDATE SET status='activa'
		 RETURNING id`, projectID, agentID, claudeSessionID).Scan(&id)
	return id, err
}

func nullStr(p *string) any {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

// WaitReady reintenta el ping hasta que Postgres responda o se agote el tiempo.
func (s *Store) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if err := s.Ping(ctx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("postgres no respondió en %s", timeout)
		}
		time.Sleep(time.Second)
	}
}
