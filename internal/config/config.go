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
	AuthUser     string
	AuthFile     string
	AuthPassword string
	HistoryLimit int
}

func Parse(args []string) (Config, error) {
	fs := flag.NewFlagSet("9flx serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var cfg Config
	fs.StringVar(&cfg.APIBase, "api-base", envOr("FLUXER_API_BASE", "https://api.fluxer.app/v1"), "Fluxer versioned API base URL")
	fs.StringVar(&cfg.Listen, "listen", "127.0.0.1:5640", "9P TCP listen address")
	fs.StringVar(&cfg.TokenFile, "token-file", "", "path to a mode-0600 Fluxer session token file")
	fs.StringVar(&cfg.AuthUser, "auth-user", "", "9P username to require (requires --auth-file)")
	fs.StringVar(&cfg.AuthFile, "auth-file", "", "path to a mode-0600 9P password file")
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
		data, err := readSecretFile(cfg.TokenFile, "token")
		if err != nil {
			return Config{}, err
		}
		cfg.Token = strings.TrimSpace(string(data))
	} else {
		cfg.Token = strings.TrimSpace(os.Getenv("FLUXER_TOKEN"))
	}
	if cfg.Token == "" {
		return Config{}, errors.New("set --token-file or FLUXER_TOKEN")
	}
	if (cfg.AuthUser == "") != (cfg.AuthFile == "") {
		return Config{}, errors.New("--auth-user and --auth-file must be used together")
	}
	if cfg.AuthFile != "" {
		data, err := readSecretFile(cfg.AuthFile, "authentication")
		if err != nil {
			return Config{}, err
		}
		cfg.AuthPassword = strings.TrimSpace(string(data))
		if cfg.AuthPassword == "" {
			return Config{}, errors.New("9P authentication password is empty")
		}
	}
	return cfg, nil
}

func readSecretFile(path, kind string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("%s file: %w", kind, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s file is not a regular file", kind)
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%s file %s must not be readable or writable by group or others", kind, path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s file: %w", kind, err)
	}
	return data, nil
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
