package worker

import (
	"context"
	"hash/fnv"
	"sync"

	"klarf-processor/config"
	"klarf-processor/db"
	"klarf-processor/logger"
	"klarf-processor/processor"
)

// ─── Job ─────────────────────────────────────────────────────────────────────

// Job 代表同一組 LOT+WAFER 下的所有 LAYER。
// 相同 LOT+WAFER 的 Job 保證由同一個 Worker 串行執行。
type Job struct {
	LotID   string
	WaferID string
	Layers  []db.Record
}

// ─── Pool ────────────────────────────────────────────────────────────────────

// Pool 管理固定數量的 Worker，以 hash-based routing 保證
// 相同 LOT+WAFER 的 Job 永遠分派給同一個 Worker。
type Pool struct {
	count    int
	channels []chan Job
	wg       sync.WaitGroup
}

// NewPool 建立並立即啟動 Worker Pool。
// ctx 取消時，Worker 會在完成當前操作後結束。
func NewPool(ctx context.Context, count int, cfg *config.Config, log *logger.Logger, database *db.DB, stats *logger.Stats) *Pool {
	proc := processor.New(cfg, database, log, stats)

	p := &Pool{
		count:    count,
		channels: make([]chan Job, count),
	}

	for i := 0; i < count; i++ {
		ch := make(chan Job, 50) // buffered channel，避免 Submit 在瞬間高負載時阻塞
		p.channels[i] = ch

		w := &worker{
			id:    i,
			jobCh: ch,
			proc:  proc,
			log:   log,
		}

		p.wg.Add(1)
		go func(w *worker) {
			defer p.wg.Done()
			w.run(ctx)
		}(w)
	}

	log.Info("worker pool started", "workers", count)
	return p
}

// Submit 根據 LOT+WAFER hash 將 Job 路由到固定的 Worker channel。
func (p *Pool) Submit(job Job) {
	idx := routeHash(job.LotID+":"+job.WaferID, p.count)
	p.channels[idx] <- job
}

// Stop 關閉所有 Worker channel，並阻塞等待所有 Worker 處理完畢。
// 應在 ctx cancel 之後呼叫，以確保 Worker 內的長時間等待能被中斷。
func (p *Pool) Stop() {
	for _, ch := range p.channels {
		close(ch)
	}
	p.wg.Wait()
}

// ─── Worker ──────────────────────────────────────────────────────────────────

type worker struct {
	id    int
	jobCh chan Job
	proc  *processor.Processor
	log   *logger.Logger
}

// run 是每個 Worker 的主迴圈：
//   - 從 channel 取 Job 並處理
//   - channel 關閉 或 ctx 取消時正常退出
func (w *worker) run(ctx context.Context) {
	w.log.Info("worker started", "worker_id", w.id)

	for {
		select {
		case job, ok := <-w.jobCh:
			if !ok {
				w.log.Info("worker channel closed, exiting", "worker_id", w.id)
				return
			}
			w.log.Info("worker picked up job",
				"worker_id", w.id,
				"lot_id", job.LotID,
				"wafer_id", job.WaferID,
				"layer_count", len(job.Layers),
			)
			w.proc.Process(ctx, job.LotID, job.WaferID, job.Layers)

		case <-ctx.Done():
			w.log.Info("worker context cancelled, exiting", "worker_id", w.id)
			return
		}
	}
}

// ─── Routing ─────────────────────────────────────────────────────────────────

// routeHash 以 FNV-32a 將 key 對應到 [0, n) 區間的固定 index。
// 相同的 key 永遠對應同一個 index，保證路由一致性。
func routeHash(key string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % n
}
