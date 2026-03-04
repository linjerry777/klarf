package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"klarf-processor/config"
	"klarf-processor/db"
	"klarf-processor/logger"
	"klarf-processor/worker"
)

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		// Logger 尚未初始化，直接輸出 JSON 格式保持一致性
		fmt.Fprintf(os.Stderr,
			"{\"level\":\"ERROR\",\"msg\":\"failed to load config\",\"error\":%q}\n", err)
		os.Exit(1)
	}

	// ── Logger ────────────────────────────────────────────────────────────────
	log := logger.New(cfg.Log)

	log.Info("KLARF processor initializing",
		"driver", cfg.Database.Driver,
		"db_host", cfg.Database.Host,
		"source_interval", cfg.Polling.SourceInterval,
		"target_interval", cfg.Polling.TargetInterval,
		"target_max_attempts", cfg.Polling.TargetMaxAttempts,
		"cmd_max_retries", cfg.Retry.MaxAttempts,
		"workers", cfg.Worker.Count,
		"temp_dir", cfg.Export.TempDir,
		"output_dir", cfg.Export.OutputDir,
		"worker_log_dir", cfg.Log.WorkerDir,
	)

	// ── Database ──────────────────────────────────────────────────────────────
	database, err := db.New(cfg.Database)
	if err != nil {
		log.Error("failed to connect to database",
			"driver", cfg.Database.Driver,
			"host", cfg.Database.Host,
			"dbname", cfg.Database.DBName,
			"error", err)
		os.Exit(1)
	}
	defer database.Close()
	log.Info("database connected",
		"driver", cfg.Database.Driver,
		"host", cfg.Database.Host,
		"dbname", cfg.Database.DBName)

	// ── Stats ─────────────────────────────────────────────────────────────────
	stats := logger.NewStats()

	// ── Context & Signal ──────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// buffer=2：第一個信號觸發優雅關閉，第二個信號強制退出
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ── Ensure directories exist ───────────────────────────────────────────────
	for _, dir := range []string{cfg.Export.TempDir, cfg.Export.OutputDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("cannot create directory",
				"path", dir, "error", err)
			os.Exit(1)
		}
		log.Info("directory ready", "path", dir)
	}

	// ── Worker Pool ───────────────────────────────────────────────────────────
	pool := worker.NewPool(ctx, cfg.Worker.Count, cfg, log, database, stats)

	// ── Run Once ──────────────────────────────────────────────────────────────
	log.Info("KLARF processor started (run-once mode) — press Ctrl+C to cancel")

	// 執行一次掃描，把所有 Job 派給 Worker
	runCycle(ctx, log, database, pool, stats)

	log.Info("all jobs submitted, waiting for workers to finish...")

	// 背景等待所有 Worker 跑完（close channels + wg.Wait）
	shutdownDone := make(chan struct{})
	go func() {
		pool.Stop()
		close(shutdownDone)
	}()

	// 等完成，或 Ctrl+C 取消
	select {
	case <-shutdownDone:
		log.Info("all workers finished")

	case sig := <-sigCh:
		log.Info("received signal, cancelling workers... (Ctrl+C again to force quit)",
			"signal", sig)
		cancel() // 通知所有 Worker 中斷 pollTarget / CMD

		select {
		case <-shutdownDone:
			log.Info("all workers finished gracefully")
		case <-sigCh:
			log.Warn("second signal received — force quit (workers may still be running)")
		}
	}

	stats.PrintSummary(log)
}

// ─── Cycle ────────────────────────────────────────────────────────────────────

// runCycle 執行一次完整的「掃描 source → 分組 → 派工到 Worker」流程。
//
// 分組規則：
//   - 以 LOT_ID + WAFER_ID 為一組
//   - 組內依 scandate ASC 排序（由 SQL 保證）
//   - 同組若出現相同 LAYER_ID（多筆記錄），只保留最舊的那筆，其餘跳過並 log
func runCycle(ctx context.Context, log *logger.Logger, database *db.DB, pool *worker.Pool, stats *logger.Stats) {
	stats.IncrCycles()
	cycleNum := stats.Cycles()
	log.Info("═══ cycle started ═══", "cycle", cycleNum)

	// Step 1: 查詢 source table（scandate 超過 7 天，依 scandate ASC 排序）
	records, err := database.QuerySource(ctx)
	if err != nil {
		log.Error("source query failed, skipping this cycle",
			"cycle", cycleNum, "error", err)
		return
	}

	if len(records) == 0 {
		log.Info("no records to process", "cycle", cycleNum)
		return
	}
	log.Info("records fetched from source",
		"cycle", cycleNum, "total", len(records))

	// Step 2: 依 LOT_ID + WAFER_ID 分組，同時對 LAYER_ID 去重
	//
	// 為什麼需要 LAYER 去重？
	//   source table 可能出現同一個 LOT+WAFER+LAYER 有多筆記錄（不同 scandate）。
	//   因為 SQL 已依 scandate ASC 排序，第一次出現的就是最舊的那筆，後續重複的跳過。
	//   同一個 layer 只需 export 一次，避免重複執行 CMD + pollTarget。
	type groupKey struct{ lot, wafer string }
	type group struct {
		key       groupKey
		layers    []db.Record
		layerSeen map[string]struct{} // 追蹤已加入的 LAYER_ID
	}

	seen := make(map[groupKey]int) // groupKey → index in groups
	var groups []group

	for _, r := range records {
		k := groupKey{r.LotID, r.WaferID}
		if idx, ok := seen[k]; ok {
			// 已有此 LOT+WAFER 群組 → 檢查 LAYER 是否重複
			if _, dup := groups[idx].layerSeen[r.LayerID]; dup {
				log.Info("duplicate LOT+WAFER+LAYER in source, skipping extra record",
					"cycle", cycleNum,
					"lot_id", r.LotID,
					"wafer_id", r.WaferID,
					"layer_id", r.LayerID,
					"scandate", r.Scandate,
				)
				continue
			}
			groups[idx].layers = append(groups[idx].layers, r)
			groups[idx].layerSeen[r.LayerID] = struct{}{}
		} else {
			// 新群組
			seen[k] = len(groups)
			groups = append(groups, group{
				key:       k,
				layers:    []db.Record{r},
				layerSeen: map[string]struct{}{r.LayerID: {}},
			})
		}
	}

	log.Info("groups formed",
		"cycle", cycleNum, "groups", len(groups))

	// Step 3: 提交 Job 到 Worker Pool（hash routing 自動分派）
	for _, g := range groups {
		job := worker.Job{
			LotID:   g.key.lot,
			WaferID: g.key.wafer,
			Layers:  g.layers,
		}
		pool.Submit(job)
		log.Info("job submitted",
			"cycle", cycleNum,
			"lot_id", job.LotID,
			"wafer_id", job.WaferID,
			"layer_count", len(job.Layers),
		)
	}

	log.Info("═══ cycle submission done ═══",
		"cycle", cycleNum, "jobs_submitted", len(groups))
}
