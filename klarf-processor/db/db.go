package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/sijms/go-ora/v2"
	"klarf-processor/config"
)

// ─── Model ───────────────────────────────────────────────────────────────────

type Record struct {
	ID       int
	LotID    string
	WaferID  string
	LayerID  string
	Scandate time.Time
}

// ─── DB ──────────────────────────────────────────────────────────────────────

type DB struct {
	conn   *sql.DB
	driver string // "mysql" or "oracle"
}

func New(cfg config.DatabaseConfig) (*DB, error) {
	conn, err := sql.Open(cfg.DriverName(), cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}

	conn.SetMaxOpenConns(10)
	conn.SetMaxIdleConns(5)
	conn.SetConnMaxLifetime(time.Hour)

	if err := conn.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}

	return &DB{conn: conn, driver: cfg.DriverName()}, nil
}

func (d *DB) Close() error {
	return d.conn.Close()
}

// ─── Queries ─────────────────────────────────────────────────────────────────

// QuerySource 回傳 source table 中 scandate 超過 7 天的所有資料。
// ctx 取消時（Ctrl+C 或 timeout），查詢立刻中止，不會無限等待 DB 回應。
func (d *DB) QuerySource(ctx context.Context) ([]Record, error) {
	// 同一 LOT+WAFER 群組內，依 scandate 由舊到新排序，確保 Worker 串行執行時順序正確。
	// MySQL : DATE_SUB(NOW(), INTERVAL 7 DAY)
	// Oracle: SYSDATE - 7
	q := `
		SELECT id, LOT_ID, WAFER_ID, LAYER_ID, scandate
		FROM   source
		WHERE  scandate < DATE_SUB(NOW(), INTERVAL 7 DAY)
		ORDER  BY LOT_ID, WAFER_ID, scandate ASC
	`
	if d.driver == "oracle" {
		q = `
		SELECT id, LOT_ID, WAFER_ID, LAYER_ID, scandate
		FROM   source
		WHERE  scandate < SYSDATE - 7
		ORDER  BY LOT_ID, WAFER_ID, scandate ASC
	`
	}
	rows, err := d.conn.QueryContext(ctx, q) // ← QueryContext：ctx 取消即返回
	if err != nil {
		return nil, fmt.Errorf("query source: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var r Record
		if err := rows.Scan(&r.ID, &r.LotID, &r.WaferID, &r.LayerID, &r.Scandate); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}
		records = append(records, r)
	}
	return records, rows.Err()
}

// ExistsInTarget 確認 target table 是否已有對應的 LOT+WAFER+LAYER 資料。
// ctx 取消時查詢立刻中止，避免 pollTarget 在 DB 無回應時無限等待。
func (d *DB) ExistsInTarget(ctx context.Context, lotID, waferID, layerID string) (bool, error) {
	// MySQL : ? placeholders
	// Oracle: :1 :2 :3 named placeholders
	q := `
		SELECT COUNT(1)
		FROM   target
		WHERE  LOT_ID = ? AND WAFER_ID = ? AND LAYER_ID = ?
	`
	if d.driver == "oracle" {
		q = `
		SELECT COUNT(1)
		FROM   target
		WHERE  LOT_ID = :1 AND WAFER_ID = :2 AND LAYER_ID = :3
	`
	}
	var count int
	// ← QueryRowContext：ctx 取消即返回，不會卡在 DB 等待
	if err := d.conn.QueryRowContext(ctx, q, lotID, waferID, layerID).Scan(&count); err != nil {
		return false, fmt.Errorf("query target: %w", err)
	}
	return count > 0, nil
}
