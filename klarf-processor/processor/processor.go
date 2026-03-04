package processor

import (
	"bytes"
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

// ─── layerStatus ─────────────────────────────────────────────────────────────

type layerStatus int

const (
	statusSuccess   layerStatus = iota // 成功產出 KLARF 且 target 已確認
	statusSkipped                      // target 已存在，跳過
	statusFailed                       // 重試耗盡後仍失敗
	statusCancelled                    // context 已取消
)

type layerResult struct {
	layerID  string
	status   layerStatus
	file     string // 成功時產出的 KLARF 路徑
}

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

// Process 依 scandate ASC 順序處理同一 LOT+WAFER 的所有 LAYER，
// 最後做 Reconciliation：比對 SQL 預期 vs 實際產出，報告漏掉的 layer。
func (p *Processor) Process(ctx context.Context, lotID, waferID string, layers []db.Record) {
	p.log.Info("group processing started",
		"lot_id", lotID,
		"wafer_id", waferID,
		"layer_count", len(layers),
	)

	results := make([]layerResult, 0, len(layers))

	for _, layer := range layers {
		if ctx.Err() != nil {
			p.log.Warn("context cancelled, aborting remaining layers",
				"lot_id", lotID, "wafer_id", waferID,
				"remaining_layer", layer.LayerID)
			results = append(results, layerResult{layer.LayerID, statusCancelled, ""})
			continue
		}
		r := p.processLayer(ctx, lotID, waferID, layer.LayerID)
		results = append(results, r)
	}

	p.reconcile(lotID, waferID, results)

	p.log.Info("group processing finished",
		"lot_id", lotID, "wafer_id", waferID)
}

// ─── Reconciliation ──────────────────────────────────────────────────────────

// reconcile 比對預期處理的 layer 數 vs 實際成功產出的數量，log 出漏掉的清單。
func (p *Processor) reconcile(lotID, waferID string, results []layerResult) {
	var produced, skipped, failed, cancelled int
	var missingLayers []string

	for _, r := range results {
		switch r.status {
		case statusSuccess:
			produced++
		case statusSkipped:
			skipped++
		case statusFailed:
			failed++
			missingLayers = append(missingLayers, r.layerID)
		case statusCancelled:
			cancelled++
			missingLayers = append(missingLayers, r.layerID)
		}
	}

	logFn := p.log.Info
	if failed > 0 || cancelled > 0 {
		logFn = p.log.Warn
	}

	logFn("group reconciliation",
		"lot_id", lotID,
		"wafer_id", waferID,
		"total_layers", len(results),
		"produced", produced,
		"skipped_already_done", skipped,
		"failed", failed,
		"cancelled", cancelled,
		"missing_layers", missingLayers,
	)

	if int64(failed+cancelled) > 0 {
		p.stats.AddLayersMissing(int64(failed + cancelled))
	}
}

// ─── Layer processing ─────────────────────────────────────────────────────────

func (p *Processor) processLayer(ctx context.Context, lotID, waferID, layerID string) layerResult {
	p.log.Info("layer processing started",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)

	// ── Step 1: 已存在於 target 則跳過 ────────────────────────────────────
	exists, err := p.db.ExistsInTarget(ctx, lotID, waferID, layerID)
	if err != nil {
		p.log.Error("target pre-check DB error, will attempt processing anyway",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"error", err)
		// DB 查詢失敗不跳過，繼續嘗試（避免因暫時性 DB 問題漏處理）
	} else if exists {
		p.log.Info("layer already exists in target, skipping",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID)
		p.stats.IncrLayersSkipped()
		return layerResult{layerID, statusSkipped, ""}
	}

	p.stats.IncrLayersTotal()

	// ── Step 2: 執行 CMD，最多重試 MaxAttempts 次 ──────────────────────────
	tempDir := p.cfg.Export.TempDir
	var foundFile string
	klarfOK := false

	for attempt := 1; attempt <= p.cfg.Retry.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			p.log.Warn("context cancelled during CMD retry",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt)
			return layerResult{layerID, statusCancelled, ""}
		}

		if attempt > 1 {
			p.stats.IncrCmdRetries()
			p.log.Warn("retrying CMD",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "max_attempts", p.cfg.Retry.MaxAttempts)
		}

		// 每次執行 CMD 前快照一次 temp_dir，偵測此次新增的檔案
		before, snapErr := p.snapshotDir(tempDir, lotID)
		if snapErr != nil {
			p.log.Error("cannot snapshot temp_dir before CMD",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"temp_dir", tempDir, "error", snapErr)
			// 快照失敗不中止，繼續執行 CMD（會用空 map 做比對，可能重複計算已存在的舊檔）
			before = map[string]struct{}{}
		}

		// 執行 export CMD
		cmdErr := p.runExport(ctx, lotID, waferID, layerID)
		if cmdErr != nil {
			p.log.Error("CMD execution failed",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "max_attempts", p.cfg.Retry.MaxAttempts,
				"error", cmdErr)
			p.stats.IncrCmdExecFail()
			continue
		}

		// 偵測 CMD 新產出的檔案（before → after 差集，只看 lotID 前綴）
		newFiles, scanErr := p.detectNewFiles(lotID, tempDir, before)
		if scanErr != nil {
			p.log.Error("cannot scan temp_dir after CMD",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"temp_dir", tempDir, "error", scanErr)
			continue
		}

		if len(newFiles) == 0 {
			// CMD exit 0 但沒有產出任何檔案
			p.log.Warn("CMD succeeded but no new KLARF file found in temp_dir",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "temp_dir", tempDir)
			p.stats.IncrFileNotFound()
			continue
		}

		// 依 KLARF header（LotID / WaferID / StepID）過濾，只保留屬於本 layer 的檔案。
		// 解決多個並行 worker 使用相同 LOT 前綴時，互相把對方的產出納入偵測範圍的問題。
		matchedFiles := p.filterByKlarfHeader(lotID, waferID, layerID, newFiles)
		if len(matchedFiles) == 0 {
			p.log.Warn("CMD succeeded but no KLARF matched this lot/wafer/layer",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "temp_dir", tempDir,
				"detected_files", newFiles)
			p.stats.IncrFileNotFound()
			continue
		}
		if len(matchedFiles) > 1 {
			p.log.Warn("multiple KLARF files matched this lot/wafer/layer (CMD may have produced duplicates)",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"files", matchedFiles)
		}

		// 對符合 header 的檔案做結構驗證（末尾必須是 EndOfFile;）
		for _, f := range matchedFiles {
			reason, valid := p.validateKlarf(f)
			if valid {
				p.log.Info("KLARF validated OK",
					"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
					"attempt", attempt, "file", f)
				p.stats.IncrKlarfSuccess()
				foundFile = f
				klarfOK = true
				break
			}
			p.log.Warn("KLARF validation failed",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt, "file", f, "reason", reason)
			p.stats.IncrKlarfInvalid()
		}

		if klarfOK {
			break
		}
	}

	if !klarfOK {
		p.log.Error("all CMD attempts exhausted, layer failed",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"max_attempts", p.cfg.Retry.MaxAttempts)
		p.stats.IncrKlarfFail()
		return layerResult{layerID, statusFailed, ""}
	}

	// ── Step 3: Poll target table ──────────────────────────────────────────
	p.pollTarget(ctx, lotID, waferID, layerID)

	return layerResult{layerID, statusSuccess, foundFile}
}

// ─── CMD execution ───────────────────────────────────────────────────────────

// runExport 執行 export 指令，輸出目錄為 temp_dir。
// export 的 stdout/stderr 透過 bytes.Buffer 捕捉後，統一用 structured log 輸出，
// 避免原始文字混入 JSON log stream。
func (p *Processor) runExport(ctx context.Context, lotID, waferID, layerID string) error {
	var stdout, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx,
		p.cfg.Export.Command,
		"--LOT_ID", lotID,
		"--WAFER_ID", waferID,
		"--LAYER_ID", layerID,
		"--output_dir", p.cfg.Export.TempDir,
	)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	p.log.Info("executing CMD",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
		"command", p.cfg.Export.Command,
		"output_dir", p.cfg.Export.TempDir,
	)

	runErr := cmd.Run()

	// 將 CMD 的 stdout/stderr 統一寫入 structured log
	if out := strings.TrimSpace(stdout.String()); out != "" {
		p.log.Info("CMD stdout",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"output", out)
	}
	if errOut := strings.TrimSpace(stderr.String()); errOut != "" {
		p.log.Warn("CMD stderr",
			"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
			"output", errOut)
	}

	if runErr != nil {
		return fmt.Errorf("exec %q: %w", p.cfg.Export.Command, runErr)
	}
	return nil
}

// ─── File detection ──────────────────────────────────────────────────────────

// snapshotDir 回傳 dir 內所有以 lotID 為前綴的檔名集合（快照）。
func (p *Processor) snapshotDir(dir, lotID string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]struct{}{}, nil // 目錄尚未建立視為空
		}
		return nil, fmt.Errorf("readdir %q: %w", dir, err)
	}

	snap := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), lotID) {
			snap[e.Name()] = struct{}{}
		}
	}
	return snap, nil
}

// detectNewFiles 比對 CMD 執行前後的目錄快照，回傳新增的、以 lotID 為前綴的檔案路徑。
// 這樣不論 export 程式使用什麼命名（LOT.001、LOT.002 …），都能偵測到。
func (p *Processor) detectNewFiles(lotID, dir string, before map[string]struct{}) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("readdir %q after CMD: %w", dir, err)
	}

	var newFiles []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, existed := before[e.Name()]; existed {
			continue
		}
		if strings.HasPrefix(e.Name(), lotID) {
			newFiles = append(newFiles, filepath.Join(dir, e.Name()))
		}
	}
	return newFiles, nil
}

// ─── KLARF validation ────────────────────────────────────────────────────────

// validateKlarf 確認檔案存在且最後一個非空行為 "EndOfFile;"。
// 只讀尾部 128 bytes，避免讀取整個大檔。
// 回傳 (失敗原因, 是否通過)，失敗原因為空字串代表通過。
func (p *Processor) validateKlarf(path string) (reason string, ok bool) {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("cannot open file: %v", err), false
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Sprintf("stat error: %v", err), false
	}
	if info.Size() == 0 {
		return "file is empty", false
	}

	const tailSize = 128
	offset := info.Size() - tailSize
	if offset < 0 {
		offset = 0
	}

	buf := make([]byte, info.Size()-offset)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return fmt.Sprintf("read error: %v", err), false
	}

	// 找最後一個非空白行
	lines := strings.Split(strings.TrimRight(string(buf), "\r\n "), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		if line == klarfEnding {
			return "", true
		}
		return fmt.Sprintf("last line is %q, expected %q", line, klarfEnding), false
	}
	return "file has no content", false
}

// ─── KLARF header matching ───────────────────────────────────────────────────

// filterByKlarfHeader 從 files 中只保留 header 欄位（LotID / WaferID / StepID）
// 與預期值相符的檔案。
//
// 為什麼要過濾？
//   snapshotDir 以 lotID 為前綴進行掃描，因此當多個 worker 同時處理相同 LOT
//   但不同 WAFER 時（例如 LOT001/WAFER01 與 LOT001/WAFER02），各 worker 的
//   detectNewFiles 差集都可能包含對方產出的 LOT001.* 檔案。
//   透過讀取前 512 bytes 並比對 WaferID / StepID，可以精準找出本 layer 的產出。
func (p *Processor) filterByKlarfHeader(lotID, waferID, layerID string, files []string) []string {
	var matched []string
	for _, f := range files {
		fLot, fWafer, fStep, err := parseKlarfHeader(f)
		if err != nil {
			// 讀不到 header 時暫時當成候選，交給後續 validateKlarf 決定
			p.log.Warn("cannot read KLARF header for matching, treating as candidate",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"file", f, "error", err)
			matched = append(matched, f)
			continue
		}
		if fLot == lotID && fWafer == waferID && fStep == layerID {
			matched = append(matched, f)
		} else {
			// 屬於其他並行 worker，靜默略過（可調高 log level 至 debug 查看）
			p.log.Debug("skipping KLARF belonging to concurrent worker",
				"expected_lot", lotID, "expected_wafer", waferID, "expected_layer", layerID,
				"file", f, "file_lot", fLot, "file_wafer", fWafer, "file_layer", fStep)
		}
	}
	return matched
}

// parseKlarfHeader 讀取 KLARF 檔案的前 512 bytes，
// 從中提取 LotID、WaferID、StepID 三個欄位的值。
// 這三個欄位均位於檔案開頭約前 15 行，512 bytes 已綽綽有餘。
func parseKlarfHeader(path string) (lotID, waferID, stepID string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", "", fmt.Errorf("cannot open: %w", err)
	}
	defer f.Close()

	// Best-effort 讀取：小檔案只會讀到 EOF，大檔案讀到 512 bytes 即可
	buf := make([]byte, 512)
	n, _ := f.Read(buf)

	for _, line := range strings.Split(string(buf[:n]), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case lotID == "" && strings.HasPrefix(line, "LotID "):
			lotID = extractKlarfQuotedValue(line)
		case waferID == "" && strings.HasPrefix(line, "WaferID "):
			waferID = extractKlarfQuotedValue(line)
		case stepID == "" && strings.HasPrefix(line, "StepID "):
			stepID = extractKlarfQuotedValue(line)
		}
		if lotID != "" && waferID != "" && stepID != "" {
			break
		}
	}
	return
}

// extractKlarfQuotedValue 從 KLARF 欄位行擷取第一個雙引號中的值。
// 例如：`LotID "LOT001";` → "LOT001"
// 對於無引號格式（StepID GATE;）則回傳空字串，不影響過濾邏輯。
func extractKlarfQuotedValue(line string) string {
	start := strings.Index(line, `"`)
	if start < 0 {
		return ""
	}
	rest := line[start+1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// ─── Target polling ──────────────────────────────────────────────────────────

// pollTarget 每隔 TargetInterval 查詢一次 target table，
// 最多 TargetMaxAttempts 次後視為 timeout。
func (p *Processor) pollTarget(ctx context.Context, lotID, waferID, layerID string) {
	p.log.Info("start polling target",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
		"interval", p.cfg.Polling.TargetInterval,
		"max_attempts", p.cfg.Polling.TargetMaxAttempts,
	)

	ticker := time.NewTicker(p.cfg.Polling.TargetInterval)
	defer ticker.Stop()

	consecutiveErrors := 0
	const maxConsecutiveErrors = 3

	for attempt := 1; attempt <= p.cfg.Polling.TargetMaxAttempts; attempt++ {
		exists, err := p.db.ExistsInTarget(ctx, lotID, waferID, layerID)
		if err != nil {
			consecutiveErrors++
			p.log.Error("target poll DB error",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt,
				"consecutive_errors", consecutiveErrors,
				"error", err)
			if consecutiveErrors >= maxConsecutiveErrors {
				p.log.Error("target poll aborted: too many consecutive DB errors",
					"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
					"consecutive_errors", consecutiveErrors)
				p.stats.IncrTargetTimeout()
				return
			}
		} else {
			consecutiveErrors = 0 // 查詢成功，重設錯誤計數
			if exists {
				p.log.Info("target confirmed ✓",
					"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
					"attempt", attempt)
				p.stats.IncrTargetSuccess()
				return
			}
			p.log.Info("not yet in target, waiting next poll",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt,
				"max_attempts", p.cfg.Polling.TargetMaxAttempts,
				"next_check_in", p.cfg.Polling.TargetInterval,
			)
		}

		// 最後一次查詢後不等待
		if attempt == p.cfg.Polling.TargetMaxAttempts {
			break
		}

		// 等待下次 tick 或 context 取消
		select {
		case <-ctx.Done():
			p.log.Warn("context cancelled during target polling",
				"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
				"attempt", attempt)
			return
		case <-ticker.C:
		}
	}

	p.log.Error("target polling timeout, layer not confirmed",
		"lot_id", lotID, "wafer_id", waferID, "layer_id", layerID,
		"max_attempts", p.cfg.Polling.TargetMaxAttempts)
	p.stats.IncrTargetTimeout()
}
