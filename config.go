package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	// The confita package supports configuration from
	// command-line, environment,yaml, and etcd & consul
	"github.com/heetch/confita"
	"github.com/heetch/confita/backend/env"
	"github.com/heetch/confita/backend/file"
	"github.com/heetch/confita/backend/flags"
)

const defaultConfigPath = "./config.yaml"

type Label struct {
	Name  string
	Value string
}

func (l Label) String() string {
	return fmt.Sprintf("%s=%s", l.Name, l.Value)
}

type ProtocolConfig struct {
	Type      string
	Enable    bool
	Host      string
	Port      int
	HTTP_path string
	Config    map[string]interface{}
	Name      string
}

type HttpConfig struct {
	Enable         bool
	Listen         string
	Telemetry_path string
}

type SinkConfig struct {
	Type   string
	Name   string
	Size   uint32
	Config map[string]interface{}
}

func (p ProtocolConfig) String() string {
	status := ""
	if !p.Enable {
		status = "(Disabled)"
	}
	if p.HTTP_path != "" {
		return fmt.Sprintf("%v HTTP Path:%s %s",
			p.Type, p.HTTP_path, status)
	}
	return fmt.Sprintf("%v TCP Host:%s  Port:%d %s",
		p.Type, p.Host, p.Port, status)
}

func (s SinkConfig) String() string {
	return fmt.Sprintf("Sink: type:%s name:%s size:%d", s.Type, s.Name, s.Size)
}

type ServerConfig struct {
	Server struct {
		Debug bool `config:"debug"`
		// time after which idle connection will be closed
		Idle_timeout_sec int64 `config:"idleTimeoutSec"`
		// if non-zero, used to set tcp connection keep alive period
		Keep_alive_period time.Duration `config:"keepAlivePeriod"`
	}

	Http HttpConfig

	Sinks     []SinkConfig
	Protocols []ProtocolConfig
	Labels    []Label

	//Enable  string `config:"enable"`
	//Disable string `config:"disable"`
}

func DefaultConfig() *ServerConfig {
	cfg := new(ServerConfig)
	cfg.Server.Debug = true
	cfg.Server.Idle_timeout_sec = 60
	cfg.Server.Keep_alive_period = 0
	cfg.Http.Enable = true
	cfg.Http.Listen = ":9201"
	cfg.Http.Telemetry_path = "/metrics"
	cfg.Sinks = make([]SinkConfig, 0, 0)

	return cfg
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Size() > 0 && !info.IsDir()
}

func getProtocolConfig(cfg *ServerConfig, name string) (p *ProtocolConfig, ok bool) {
	for i := range cfg.Protocols {
		if string(cfg.Protocols[i].Type) == name {
			return &cfg.Protocols[i], true
		}
	}
	return nil, false
}

func ensureConfig(paths []string, cfg *SinkConfig) error {
	for _, p := range paths {
		if getConfigString(p, cfg.Config) == "" {
			s := fmt.Sprintf("Missing Config.%s for output Type %s Name %s", p, cfg.Type, cfg.Name)
			return errors.New(s)
		}
	}
	return nil
}

// LoadConfig loads configuration from config file,
// environment variables, and command line.
// Environment and command line override file
//
//   The command line parameters protocol.enable and protocol.disable
//	 can be used to provide a comma-separated list of protocols
//	 that override the 'enable' flag in the yaml file.
func LoadConfig(ctx context.Context) (*ServerConfig, error) {

	cfg := DefaultConfig()

	cfile := os.Getenv("CONFIG")
	if cfile == "" {
		cfile = defaultConfigPath
	}
	if !fileExists(cfile) {
		msg := fmt.Sprintf("Missing configuration file. Please set CONFIG environment variable or place file in %s", defaultConfigPath)
		return nil, errors.New(msg)
	}

	fmt.Printf("Configuration loaded from %s\n", cfile)

	// don't fail on error
	err := confita.NewLoader(
		flags.NewBackend(),
		env.NewBackend(),
		file.NewBackend(cfile),
	).Load(ctx, cfg)

	if err != nil {
		fmt.Printf("Config warning: %v\n", err)
	}

	if cfg.Server.Keep_alive_period < 0 {
		return nil, errors.New("KeepAlivePeriod may not be negative")
	}
	if cfg.Server.Idle_timeout_sec <= 0 {
		return nil, errors.New("IdleTimeoutSec must be positive")
	}

	// don't fail on error
	// handle enable/disable overrides
	/*
		for _, s := range strings.Split(cfg.Enable, ",") {
			for i, _ := range cfg.Protocols {
				if string(cfg.Protocols[i].Type) == s {
					cfg.Protocols[i].Enable = true
				}
			}
		}

		for _, s := range strings.Split(cfg.Disable, ",") {
			for i := range cfg.Protocols {
				if string(cfg.Protocols[i].Type) == s {
					cfg.Protocols[i].Enable = false
				}
			}
		}
	*/
	return cfg, err
}
