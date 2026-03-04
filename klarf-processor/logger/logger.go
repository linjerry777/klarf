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

// New 建立主 logger（stdout + 可選的 log 檔案）。
func New(cfg config.LogConfig) *Logger {
	return newLogger(cfg.Level, buildWriters(cfg.File, "main logger"))
}

// NewWorkerLogger 建立 worker 專屬 logger。
// 輸出到：stdout（帶 worker_id 標籤）+ logs/workers/worker_{id}.log（可選）
func NewWorkerLogger(workerID int, cfg config.LogConfig) *Logger {
	var workerFile string
	if cfg.WorkerDir != "" {
		if err := os.MkdirAll(cfg.WorkerDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "[logger] cannot create worker log dir %s: %v\n", cfg.WorkerDir, err)
		} else {
			workerFile = filepath.Join(cfg.WorkerDir, fmt.Sprintf("worker_%d.log", workerID))
		}
	}

	writers := buildWriters(workerFile, fmt.Sprintf("worker_%d logger", workerID))
	level := parseLevel(cfg.Level)

	h := slog.NewJSONHandler(
		io.MultiWriter(writers...),
		&slog.HandlerOptions{Level: level},
	)
	// 每筆 log 自動帶上 worker_id，無需手動傳遞
	inner := slog.New(h).With("worker_id", workerID)
	return &Logger{inner}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func buildWriters(filePath, label string) []io.Writer {
	writers := []io.Writer{os.Stdout}

	if filePath == "" {
		return writers
	}

	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[logger] cannot create log dir %s (%s): %v\n", dir, label, err)
		return writers
	}

	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[logger] cannot open log file %s (%s): %v\n", filePath, label, err)
		return writers
	}

	fmt.Fprintf(os.Stdout, "{\"level\":\"INFO\",\"msg\":\"log file opened\",\"path\":%q,\"logger\":%q}\n", filePath, label)
	return append(writers, f)
}

func newLogger(levelStr string, writers []io.Writer) *Logger {
	h := slog.NewJSONHandler(
		io.MultiWriter(writers...),
		&slog.HandlerOptions{Level: parseLevel(levelStr)},
	)
	return &Logger{slog.New(h)}
}

func parseLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// ─── Stats ───────────────────────────────────────────────────────────────────

// Stats 以 atomic 操作追蹤各類計數，執行緒安全。
type Stats struct {
	cycles         atomic.Int64 // 總掃描循環數
	layersTotal    atomic.Int64 // 送出處理的 layer 總數（不含 skipped）
	layersSkipped  atomic.Int64 // target 已存在，跳過
	cmdExecFail    atomic.Int64 // CMD 執行失敗（exit code != 0）
	fileNotFound   atomic.Int64 // CMD 成功但 klarf_temp 找不到任何新檔案
	klarfInvalid   atomic.Int64 // 找到檔案但末尾不是 EndOfFile;
	klarfSuccess   atomic.Int64 // KLARF 驗證通過
	klarfFail      atomic.Int64 // 最終失敗（重試耗盡後仍未產出有效 KLARF）
	targetSuccess  atomic.Int64 // target table 確認寫入
	targetTimeout  atomic.Int64 // target poll 超時
	cmdRetries     atomic.Int64 // CMD 重試累計次數
	layersMissing  atomic.Int64 // reconciliation：漏掉的 layer 數
}

func NewStats() *Stats { return &Stats{} }

func (s *Stats) IncrCycles()        { s.cycles.Add(1) }
func (s *Stats) IncrLayersTotal()   { s.layersTotal.Add(1) }
func (s *Stats) IncrLayersSkipped() { s.layersSkipped.Add(1) }
func (s *Stats) IncrCmdExecFail()   { s.cmdExecFail.Add(1) }
func (s *Stats) IncrFileNotFound()  { s.fileNotFound.Add(1) }
func (s *Stats) IncrKlarfInvalid()  { s.klarfInvalid.Add(1) }
func (s *Stats) IncrKlarfSuccess()  { s.klarfSuccess.Add(1) }
func (s *Stats) IncrKlarfFail()     { s.klarfFail.Add(1) }
func (s *Stats) IncrTargetSuccess() { s.targetSuccess.Add(1) }
func (s *Stats) IncrTargetTimeout() { s.targetTimeout.Add(1) }
func (s *Stats) IncrCmdRetries()    { s.cmdRetries.Add(1) }
func (s *Stats) AddLayersMissing(n int64) { s.layersMissing.Add(n) }
func (s *Stats) Cycles() int64      { return s.cycles.Load() }

// PrintSummary 輸出結構化 log 與人類可讀的統計表格。
func (s *Stats) PrintSummary(log *Logger) {
	log.Info("final statistics",
		"total_cycles",     s.cycles.Load(),
		"layers_total",     s.layersTotal.Load(),
		"layers_skipped",   s.layersSkipped.Load(),
		"cmd_exec_fail",    s.cmdExecFail.Load(),
		"file_not_found",   s.fileNotFound.Load(),
		"klarf_invalid",    s.klarfInvalid.Load(),
		"klarf_success",    s.klarfSuccess.Load(),
		"klarf_fail",       s.klarfFail.Load(),
		"target_confirmed", s.targetSuccess.Load(),
		"target_timeout",   s.targetTimeout.Load(),
		"cmd_retries",      s.cmdRetries.Load(),
		"layers_missing",   s.layersMissing.Load(),
		"timestamp",        time.Now().Format(time.RFC3339),
	)

	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════╗")
	fmt.Println("║          KLARF Processor  Summary             ║")
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  Total Cycles         : %-20d ║\n", s.cycles.Load())
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  Layers Total         : %-20d ║\n", s.layersTotal.Load())
	fmt.Printf( "║  Layers Skipped       : %-20d ║\n", s.layersSkipped.Load())
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  CMD Exec Fail        : %-20d ║\n", s.cmdExecFail.Load())
	fmt.Printf( "║  File Not Produced    : %-20d ║\n", s.fileNotFound.Load())
	fmt.Printf( "║  KLARF Invalid Format : %-20d ║\n", s.klarfInvalid.Load())
	fmt.Printf( "║  CMD Retries (total)  : %-20d ║\n", s.cmdRetries.Load())
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  KLARF Success        : %-20d ║\n", s.klarfSuccess.Load())
	fmt.Printf( "║  KLARF Failed         : %-20d ║\n", s.klarfFail.Load())
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  Target Confirmed     : %-20d ║\n", s.targetSuccess.Load())
	fmt.Printf( "║  Target Timeout       : %-20d ║\n", s.targetTimeout.Load())
	fmt.Println("╠═══════════════════════════════════════════════╣")
	fmt.Printf( "║  Layers Missing (recon): %-19d ║\n", s.layersMissing.Load())
	fmt.Println("╚═══════════════════════════════════════════════╝")
	fmt.Println()
}
