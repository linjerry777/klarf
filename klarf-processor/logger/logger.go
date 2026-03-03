package logger

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"klarf-processor/config"
)

// ─── Logger ──────────────────────────────────────────────────────────────────

// Logger 封裝 slog.Logger，同時輸出 JSON 到 stdout 及可選的 log 檔案。
type Logger struct {
	*slog.Logger
}

func New(cfg config.LogConfig) *Logger {
	writers := []io.Writer{os.Stdout}

	if cfg.File != "" {
		dir := filepath.Dir(cfg.File)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "[logger] cannot create log dir %s: %v\n", dir, err)
		} else {
			f, err := os.OpenFile(cfg.File, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[logger] cannot open log file %s: %v\n", cfg.File, err)
			} else {
				writers = append(writers, f)
				fmt.Fprintf(os.Stdout, "{\"level\":\"INFO\",\"msg\":\"log file opened\",\"path\":%q}\n", cfg.File)
			}
		}
	}

	var level slog.Level
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	h := slog.NewJSONHandler(
		io.MultiWriter(writers...),
		&slog.HandlerOptions{Level: level},
	)
	return &Logger{slog.New(h)}
}

// ─── Stats ───────────────────────────────────────────────────────────────────

// Stats 以 atomic 操作追蹤各類計數，執行緒安全。
type Stats struct {
	cycles        atomic.Int64 // 總掃描循環數
	klarfSuccess  atomic.Int64 // KLARF 驗證成功
	klarfFail     atomic.Int64 // KLARF 驗證最終失敗（重試耗盡）
	targetSuccess atomic.Int64 // target table 確認寫入成功
	targetTimeout atomic.Int64 // target polling 超時
	cmdRetries    atomic.Int64 // CMD 重試累計次數
}

func NewStats() *Stats { return &Stats{} }

func (s *Stats) IncrCycles()        { s.cycles.Add(1) }
func (s *Stats) IncrKlarfSuccess()  { s.klarfSuccess.Add(1) }
func (s *Stats) IncrKlarfFail()     { s.klarfFail.Add(1) }
func (s *Stats) IncrTargetSuccess() { s.targetSuccess.Add(1) }
func (s *Stats) IncrTargetTimeout() { s.targetTimeout.Add(1) }
func (s *Stats) IncrCmdRetries()    { s.cmdRetries.Add(1) }
func (s *Stats) Cycles() int64      { return s.cycles.Load() }

// PrintSummary 輸出結構化 log 與人類可讀的統計表格。
func (s *Stats) PrintSummary(log *Logger) {
	log.Info("final statistics",
		"total_cycles",     s.cycles.Load(),
		"klarf_success",    s.klarfSuccess.Load(),
		"klarf_fail",       s.klarfFail.Load(),
		"target_confirmed", s.targetSuccess.Load(),
		"target_timeout",   s.targetTimeout.Load(),
		"cmd_retries",      s.cmdRetries.Load(),
		"timestamp",        time.Now().Format(time.RFC3339),
	)

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════╗")
	fmt.Println("║         KLARF Processor  Summary          ║")
	fmt.Println("╠═══════════════════════════════════════════╣")
	fmt.Printf("║  Total Cycles       : %-20d ║\n", s.cycles.Load())
	fmt.Printf("║  KLARF Generated    : %-20d ║\n", s.klarfSuccess.Load())
	fmt.Printf("║  KLARF Failed       : %-20d ║\n", s.klarfFail.Load())
	fmt.Printf("║  Target Confirmed   : %-20d ║\n", s.targetSuccess.Load())
	fmt.Printf("║  Target Timeout     : %-20d ║\n", s.targetTimeout.Load())
	fmt.Printf("║  CMD Retries        : %-20d ║\n", s.cmdRetries.Load())
	fmt.Println("╚═══════════════════════════════════════════╝")
	fmt.Println()
}
