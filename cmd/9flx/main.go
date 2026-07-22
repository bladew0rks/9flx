package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bladew0rks/9flx/internal/config"
	"github.com/bladew0rks/9flx/internal/core"
	"github.com/bladew0rks/9flx/internal/fluxer"
	"github.com/bladew0rks/9flx/internal/p9fs"
)

type disconnectLogFilter struct {
	destination io.Writer
}

func (w disconnectLogFilter) Write(record []byte) (int, error) {
	if bytes.Contains(record, []byte("Protocol error: EOF\n")) {
		return len(record), nil
	}
	return w.destination.Write(record)
}

func main() {
	log.SetOutput(disconnectLogFilter{destination: os.Stderr})
	if err := run(os.Args[1:]); err != nil {
		log.Printf("9flx: %v", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "serve" {
		return errors.New("usage: 9flx serve [--api-base URL] [--listen ADDRESS] [--token-file PATH] [--history-limit N]")
	}
	cfg, err := config.Parse(args[1:])
	if err != nil {
		return err
	}
	if host, _, splitErr := net.SplitHostPort(cfg.Listen); splitErr == nil && host != "127.0.0.1" && host != "localhost" && host != "::1" {
		log.Printf("warning: listening on non-loopback address %s exposes your Fluxer account to network 9P clients", cfg.Listen)
	}
	status := core.NewStatus()
	api, err := fluxer.NewClient(cfg.APIBase, cfg.Token, fluxer.WithRequestObserver(status.ObserveREST))
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	startup, startupCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer startupCancel()
	discovery, err := api.Discovery(startup)
	if err != nil {
		return fmt.Errorf("instance discovery: %w", err)
	}
	if discovery.Endpoints.Gateway == "" {
		return errors.New("instance discovery returned no gateway endpoint")
	}
	hub := core.NewHub(status.Overflow)
	store := core.NewStore(hub)
	if err := store.Bootstrap(startup, api); err != nil {
		return err
	}
	gateway := &fluxer.Gateway{
		URL: discovery.Endpoints.Gateway, Token: cfg.Token,
		OnEvent:     store.ApplyGateway,
		OnState:     status.SetGateway,
		OnError:     status.Error,
		OnReconnect: status.Reconnected,
		OnGap:       func() { hub.GapAll("gateway_session_gap") },
	}
	settings, err := api.Settings(startup)
	if err != nil {
		return fmt.Errorf("user settings: %w", err)
	}
	if err := gateway.SetPresence(settings.Status); err != nil {
		return err
	}
	if err := gateway.SetCustomStatus(settings.CustomStatus); err != nil {
		return err
	}
	tree, err := p9fs.NewTree(api, store, hub, status, gateway.SetPresence, cfg.HistoryLimit)
	if err != nil {
		return err
	}
	errCh := make(chan error, 2)
	go func() { errCh <- gateway.Run(ctx) }()
	go func() { errCh <- p9fs.Serve(ctx, cfg.Listen, tree) }()
	log.Printf("serving Fluxer over 9P2000 at %s", cfg.Listen)
	err = <-errCh
	cancel()
	if err != nil {
		return err
	}
	return nil
}
