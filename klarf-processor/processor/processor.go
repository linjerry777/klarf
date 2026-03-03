package processor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"klarf-processor/config"
	"klarf-processor/db"
	"klarf-processor/logger"
)

const klarfEnding = "EndOfFile;" // KLARF 1.1 規格：結尾行含分號

// ─── Processor ───────────────────────────────────────────────────────────────

type Processor struct {
	cfg   *config.Config
	db    *db.DB
	log   *logger.Logger
	stats *logger.Stats
}

func New(cfg *config.Config, database *db.DB, log *logger.Logger, stats *logger.Stats) *Processor {
	return &Processor{cfg: cfg, db: database, log: log, stats: stats}
}

// ─── Public ──────────────────────────────────────────────────────────────────

// Process 依序處理同一組 LOT+WAFER 下的所有 LAYER。
// 同一 Worker 保證串行執行，不會並行處理同組資料。
func (p *Processor) Process(ctx context.Context, lotID, waferID string, layers []db.Record) {
	p.log.Info("group processing started",
		"lot_id", lotID,
		"wafer_id", waferID,
		"layer_count", len(layers),
	)

	for _, layer := range layers {
		if ctx.Err() != nil {
			p.log.Warn("context cancelled, abort remaining layers",
				"lot_id", lotID, "wafer_id", waferID)
			return
		}
		p.processLayer(ctx, lotID, waferID, layer.LayerID)
	}

	p.log.Info("group processing finished",
		"lot_id", lotID, "wafer_id", waferID)
}

// ─── Private ─────────────────────────────────────────────────────────────────

func (p *Processor) processLayer(ctx context.Context, lotID, waferID, layerID string) {
	p.log.Info("layer processing started",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)

	// ── Step 1: target 已存在則跳過 ────────────────────────────────────────
	exists, err := p.db.ExistsInTarget(lotID, waferID, layerID)
	if err != nil {
		p.log.Error("target pre-check failed",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"error", err)
		// 不因查詢失敗而跳過，繼續嘗試
	} else if exists {
		p.log.Info("already exists in target, skipping",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)
		return
	}

	// ── Step 2: 執行 CMD，最多重試 MaxAttempts 次 ──────────────────────────
	klarfPath := p.klarfFilePath(lotID, waferID, layerID)
	klarfOK := false

	for attempt := 1; attempt <= p.cfg.Retry.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			p.log.Warn("context cancelled during CMD retry",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)
			return
		}

		if attempt > 1 {
			p.stats.IncrCmdRetries()
			p.log.Warn("retrying CMD",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "max_attempts", p.cfg.Retry.MaxAttempts)
		}

		if err := p.runExport(ctx, lotID, waferID, layerID); err != nil {
			p.log.Error("CMD execution failed",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "error", err)
			continue
		}

		if p.validateKlarf(klarfPath) {
			p.log.Info("KLARF validated OK",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "path", klarfPath)
			p.stats.IncrKlarfSuccess()
			klarfOK = true
			break
		}

		p.log.Warn("KLARF missing EndOfFile marker",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"attempt", attempt, "path", klarfPath)
	}

	if !klarfOK {
		p.log.Error("all CMD attempts exhausted, layer failed",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"max_attempts", p.cfg.Retry.MaxAttempts)
		p.stats.IncrKlarfFail()
		return
	}

	// ── Step 3: Poll target table ──────────────────────────────────────────
	p.pollTarget(ctx, lotID, waferID, layerID)
}

// runExport 執行 export CMD 指令並等待完成。
// 額外傳入 --output_dir 讓 mock/真實 export 知道要寫到哪個資料夾。
func (p *Processor) runExport(ctx context.Context, lotID, waferID, layerID string) error {
	cmdStr := fmt.Sprintf("%s --LOT_ID %s --WAFER_ID %s --LAYER_ID %s --output_dir %s",
		p.cfg.Export.Command, lotID, waferID, layerID, p.cfg.Export.OutputDir)

	p.log.Info("executing CMD", "cmd", cmdStr)

	cmd := exec.CommandContext(ctx,
		p.cfg.Export.Command,
		"--LOT_ID", lotID,
		"--WAFER_ID", waferID,
		"--LAYER_ID", layerID,
		"--output_dir", p.cfg.Export.OutputDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("exec %q: %w", cmdStr, err)
	}
	return nil
}

// klarfFilePath 回傳預期的 KLARF 檔案路徑。
// 命名規則：{OutputDir}/{LOT_ID}_{WAFER_ID}_{LAYER_ID}.klarf
func (p *Processor) klarfFilePath(lotID, waferID, layerID string) string {
	filename := fmt.Sprintf("%s_%s_%s.klarf", lotID, waferID, layerID)
	return filepath.Join(p.cfg.Export.OutputDir, filename)
}

// validateKlarf 確認檔案存在且最後一個非空行為 "EndOfFile"。
// 只讀尾部 128 bytes，避免讀取整個大檔案。
func (p *Processor) validateKlarf(path string) bool {
	f, err := os.Open(path)
	if err != nil {
		p.log.Error("cannot open KLARF file", "path", path, "error", err)
		return false
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		p.log.Error("KLARF file empty or unreadable", "path", path)
		return false
	}

	const tailSize = 128
	size := info.Size()
	offset := size - tailSize
	if offset < 0 {
		offset = 0
	}

	buf := make([]byte, size-offset)
	if _, err := f.ReadAt(buf, offset); err != nil {
		p.log.Error("KLARF file read error", "path", path, "error", err)
		return false
	}

	// 找最後一個非空白行
	lines := strings.Split(strings.TrimRight(string(buf), "\r\n "), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line == klarfEnding
		}
	}
	return false
}

// pollTarget 每隔 TargetInterval 查詢一次 target，
// 最多 TargetMaxAttempts 次後視為 timeout。
func (p *Processor) pollTarget(ctx context.Context, lotID, waferID, layerID string) {
	p.log.Info("start polling target",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
		"interval", p.cfg.Polling.TargetInterval,
		"max_attempts", p.cfg.Polling.TargetMaxAttempts,
	)

	ticker := time.NewTicker(p.cfg.Polling.TargetInterval)
	defer ticker.Stop()

	for attempt := 1; attempt <= p.cfg.Polling.TargetMaxAttempts; attempt++ {
		exists, err := p.db.ExistsInTarget(lotID, waferID, layerID)
		if err != nil {
			p.log.Error("target poll query error",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "error", err)
		} else if exists {
			p.log.Info("target confirmed ✓",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt)
			p.stats.IncrTargetSuccess()
			return
		} else {
			p.log.Info("not yet in target, waiting next poll",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt,
				"max_attempts", p.cfg.Polling.TargetMaxAttempts,
				"next_check_in", p.cfg.Polling.TargetInterval,
			)
		}

		// 最後一次查詢完不需等待
		if attempt == p.cfg.Polling.TargetMaxAttempts {
			break
		}

		// 等待下次 tick 或 context 取消
		select {
		case <-ctx.Done():
			p.log.Warn("context cancelled during target polling",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)
			return
		case <-ticker.C:
		}
	}

	p.log.Error("target polling timeout, layer not confirmed",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
		"max_attempts", p.cfg.Polling.TargetMaxAttempts)
	p.stats.IncrTargetTimeout()
}
