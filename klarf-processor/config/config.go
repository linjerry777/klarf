package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// ─── Top-level ───────────────────────────────────────────────────────────────

type Config struct {
	Database DatabaseConfig `yaml:"database"`
	Worker   WorkerConfig   `yaml:"worker"`
	Export   ExportConfig   `yaml:"export"`
	Polling  PollingConfig  `yaml:"-"` // populated after duration parsing
	Retry    RetryConfig    `yaml:"retry"`
	Log      LogConfig      `yaml:"log"`
}

// ─── Sub-configs ─────────────────────────────────────────────────────────────

type DatabaseConfig struct {
	Driver   string `yaml:"driver"`   // "mysql"（預設）或 "oracle"
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`   // MySQL: database name / Oracle: service name
}

// DriverName 回傳 database/sql 使用的 driver 名稱。
func (d DatabaseConfig) DriverName() string {
	if d.Driver == "oracle" {
		return "oracle"
	}
	return "mysql"
}

// DSN 依照 driver 類型建立對應的連線字串。
//
//	MySQL  : user:pass@tcp(host:port)/dbname?parseTime=true&charset=utf8mb4
//	Oracle : oracle://user:pass@host:port/service_name
func (d DatabaseConfig) DSN() string {
	switch d.Driver {
	case "oracle":
		return fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
			d.User, d.Password, d.Host, d.Port, d.DBName)
	default: // mysql
		return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
			d.User, d.Password, d.Host, d.Port, d.DBName)
	}
}

type WorkerConfig struct {
	Count int `yaml:"count"`
}

type ExportConfig struct {
	Command   string `yaml:"command"`
	OutputDir string `yaml:"output_dir"`
}

type PollingConfig struct {
	SourceInterval    time.Duration
	TargetInterval    time.Duration
	TargetMaxAttempts int
}

type RetryConfig struct {
	MaxAttempts int `yaml:"max_attempts"`
}

type LogConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// ─── Raw YAML (duration fields as string) ────────────────────────────────────

type rawPolling struct {
	SourceInterval    string `yaml:"source_interval"`
	TargetInterval    string `yaml:"target_interval"`
	TargetMaxAttempts int    `yaml:"target_max_attempts"`
}

type rawConfig struct {
	Database DatabaseConfig `yaml:"database"`
	Worker   WorkerConfig   `yaml:"worker"`
	Export   ExportConfig   `yaml:"export"`
	Polling  rawPolling     `yaml:"polling"`
	Retry    RetryConfig    `yaml:"retry"`
	Log      LogConfig      `yaml:"log"`
}

// ─── Load ────────────────────────────────────────────────────────────────────

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var raw rawConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	// Parse duration strings (e.g. "5m", "1m")
	srcInterval, err := time.ParseDuration(raw.Polling.SourceInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid source_interval %q: %w", raw.Polling.SourceInterval, err)
	}
	tgtInterval, err := time.ParseDuration(raw.Polling.TargetInterval)
	if err != nil {
		return nil, fmt.Errorf("invalid target_interval %q: %w", raw.Polling.TargetInterval, err)
	}

	cfg := &Config{
		Database: raw.Database,
		Worker:   raw.Worker,
		Export:   raw.Export,
		Retry:    raw.Retry,
		Log:      raw.Log,
		Polling: PollingConfig{
			SourceInterval:    srcInterval,
			TargetInterval:    tgtInterval,
			TargetMaxAttempts: raw.Polling.TargetMaxAttempts,
		},
	}

	// Defaults
	if cfg.Worker.Count <= 0 {
		cfg.Worker.Count = 6
	}
	if cfg.Polling.SourceInterval == 0 {
		cfg.Polling.SourceInterval = 5 * time.Minute
	}
	if cfg.Polling.TargetInterval == 0 {
		cfg.Polling.TargetInterval = time.Minute
	}
	if cfg.Polling.TargetMaxAttempts <= 0 {
		cfg.Polling.TargetMaxAttempts = 10
	}
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = 3
	}
	if cfg.Export.OutputDir == "" {
		cfg.Export.OutputDir = "."
	}

	return cfg, nil
}
