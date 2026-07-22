package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	APIBase      string
	Listen       string
	Token        string
	TokenFile    string
	HistoryLimit int
}

func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("9flx serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var cfg Config
	fs.StringVar(&cfg.APIBase, "api-base", envOr("FLUXER_API_BASE", "https://api.fluxer.app/v1"), "Fluxer versioned API base URL")
	fs.StringVar(&cfg.Listen, "listen", "127.0.0.1:5640", "9P TCP listen address")
	fs.StringVar(&cfg.TokenFile, "token-file", "", "path to a mode-0600 Fluxer session token file")
	fs.IntVar(&cfg.HistoryLimit, "history-limit", 100, "messages returned by each history snapshot (1-100)")
	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() != 0 {
		return Config{}, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if cfg.HistoryLimit < 1 || cfg.HistoryLimit > 100 {
		return Config{}, errors.New("--history-limit must be between 1 and 100")
	}
	if cfg.TokenFile != "" {
		info, err := os.Stat(cfg.TokenFile)
		if err != nil {
			return Config{}, fmt.Errorf("token file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return Config{}, errors.New("token file is not a regular file")
		}
		if info.Mode().Perm()&0077 != 0 {
			return Config{}, fmt.Errorf("token file %s must not be readable or writable by group or others", cfg.TokenFile)
		}
		data, err := os.ReadFile(cfg.TokenFile)
		if err != nil {
			return Config{}, fmt.Errorf("read token file: %w", err)
		}
		cfg.Token = strings.TrimSpace(string(data))
	} else {
		cfg.Token = strings.TrimSpace(os.Getenv("FLUXER_TOKEN"))
	}
	if cfg.Token == "" {
		return Config{}, errors.New("set --token-file or FLUXER_TOKEN")
	}
	return cfg, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
