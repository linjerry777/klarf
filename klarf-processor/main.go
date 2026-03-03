package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"klarf-processor/config"
	"klarf-processor/db"
	"klarf-processor/logger"
	"klarf-processor/worker"
)

func main() {
	// ── Config ────────────────────────────────────────────────────────────────
	cfg, err := config.Load("config/config.yaml")
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// ── Logger ────────────────────────────────────────────────────────────────
	log := logger.New(cfg.Log)

	log.Info("KLARF processor initializing",
		"source_interval", cfg.Polling.SourceInterval,
		"target_interval", cfg.Polling.TargetInterval,
		"target_max_attempts", cfg.Polling.TargetMaxAttempts,
		"cmd_max_retries", cfg.Retry.MaxAttempts,
		"workers", cfg.Worker.Count,
		"output_dir", cfg.Export.OutputDir,
	)

	// ── Database ──────────────────────────────────────────────────────────────
	database, err := db.New(cfg.Database)
	if err != nil {
		log.Error("failed to connect to database",
			"host", cfg.Database.Host,
			"dbname", cfg.Database.DBName,
			"error", err)
		os.Exit(1)
	}
	defer database.Close()
	log.Info("database connected",
		"host", cfg.Database.Host, "dbname", cfg.Database.DBName)

	// ── Stats ─────────────────────────────────────────────────────────────────
	stats := logger.NewStats()

	// ── Context & Signal ──────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ── Ensure output directory exists ────────────────────────────────────────
	if err := os.MkdirAll(cfg.Export.OutputDir, 0o755); err != nil {
		log.Error("cannot create output_dir",
			"path", cfg.Export.OutputDir, "error", err)
		os.Exit(1)
	}

	// ── Worker Pool ───────────────────────────────────────────────────────────
	pool := worker.NewPool(ctx, cfg.Worker.Count, cfg, log, database, stats)

	// ── Main Loop ─────────────────────────────────────────────────────────────
	log.Info("KLARF processor started — press Ctrl+C to stop")

	// 啟動後立即執行第一個 cycle，不需等待第一個 tick
	runCycle(log, database, pool, stats)

	ticker := time.NewTicker(cfg.Polling.SourceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			runCycle(log, database, pool, stats)

		case sig := <-sigCh:
			log.Info("received signal, shutting down gracefully", "signal", sig)
			cancel()    // 通知 Worker 中斷 pollTarget 的 sleep
			pool.Stop() // 阻塞等待所有 Worker 當前 Job 完成
			stats.PrintSummary(log)
			return
		}
	}
}

// ─── Cycle ────────────────────────────────────────────────────────────────────

// runCycle 執行一次完整的「掃描 source → 分組 → 派工到 Worker」流程。
func runCycle(log *logger.Logger, database *db.DB, pool *worker.Pool, stats *logger.Stats) {
	stats.IncrCycles()
	cycleNum := stats.Cycles()
	log.Info("═══ cycle started ═══", "cycle", cycleNum)

	// Step 1: 查詢 source table（scandate 超過 7 天）
	records, err := database.QuerySource()
	if err != nil {
		log.Error("source query failed", "cycle", cycleNum, "error", err)
		return
	}

	if len(records) == 0 {
		log.Info("no records to process", "cycle", cycleNum)
		return
	}
	log.Info("records fetched from source",
		"cycle", cycleNum, "total", len(records))

	// Step 2: 依 LOT_ID + WAFER_ID 分組
	groups := make(map[string][]db.Record)
	for _, r := range records {
		key := r.LotID + ":" + r.WaferID
		groups[key] = append(groups[key], r)
	}
	log.Info("groups formed",
		"cycle", cycleNum, "groups", len(groups))

	// Step 3: 提交 Job 到 Worker Pool（hash routing 自動分派）
	for _, layers := range groups {
		job := worker.Job{
			LotID:   layers[0].LotID,
			WaferID: layers[0].WaferID,
			Layers:  layers,
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
