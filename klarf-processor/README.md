# KLARF Processor

自動化半導體晶圓檢測資料處理程式。定期從 `source` 資料庫查詢待處理資料，
透過 Worker Pool 並行執行 export 指令產出 KLARF 文件，並確認結果寫入 `target` 資料庫。

---

## 目錄結構

```
klarf-processor/
├── main.go                  # 主程式：啟動、主迴圈、優雅關閉
├── config/
│   ├── config.yaml          # 所有可設定參數
│   └── config.go            # 設定檔載入與解析
├── db/
│   └── db.go                # MySQL / Oracle 連線、QuerySource、ExistsInTarget
├── logger/
│   └── logger.go            # slog 結構化 JSON log + Stats 計數器
├── worker/
│   └── pool.go              # Worker Pool（6 worker, FNV hash routing）
├── processor/
│   └── processor.go         # 核心流程：CMD → KLARF 驗證 → Target poll
├── mock_export/
│   └── main.go              # 模擬 export 指令，產生標準 KLARF 1.1 文件
├── docker-compose.yml       # MySQL 容器
└── init.sql                 # Table schema + 測試資料（20 筆）
```

---

## 系統架構

```mermaid
flowchart TD
    subgraph DB["MySQL / Oracle Database"]
        SRC[(source table)]
        TGT[(target table)]
    end

    subgraph APP["KLARF Processor (Go)"]
        MAIN[main.go\n主迴圈 / 5分鐘 ticker]
        POOL[Worker Pool\n6 Workers]
        PROC[Processor\n核心處理邏輯]
    end

    subgraph FS["File System"]
        EXP[./export\nCMD 指令]
        KLARF[klarf_output/\nKLARF 文件]
        LOG[logs/klarf.log\nJSON 結構化 log]
    end

    SRC -- "scandate < NOW-7天" --> MAIN
    MAIN -- "hash(LOT+WAFER) % 6" --> POOL
    POOL --> PROC
    PROC -- "執行 CMD" --> EXP
    EXP -- "產出" --> KLARF
    KLARF -- "驗證 EndOfFile;" --> PROC
    PROC -- "poll 每分鐘" --> TGT
    APP -- "JSON log" --> LOG
```



---

## 主迴圈流程

```mermaid
flowchart TD
    A([程式啟動]) --> B[載入 config.yaml]
    B --> C[連接 MySQL]
    C --> D[啟動 6 個 Worker]
    D --> E[建立輸出目錄]
    E --> F[立即執行 Cycle #1]
    F --> G{等待事件}
    G -- "每 5 分鐘 ticker" --> H[執行新 Cycle]
    H --> I[QuerySource\nscandate < NOW-7天]
    I --> J{有資料?}
    J -- "無" --> G
    J -- "有 N 筆" --> K[依 LOT+WAFER 分組]
    K --> L[hash 路由\n投遞到對應 Worker]
    L --> G
    G -- "SIGINT / SIGTERM" --> M[cancel context]
    M --> N[等待所有 Worker 完成]
    N --> O[印出統計摘要]
    O --> P([程式結束])
```



---

## 單一 Layer 處理流程

```mermaid
flowchart TD
    A([Worker 收到 Job]) --> B[依序處理每個 LAYER_ID]
    B --> C{Target 已存在?}
    C -- "是（已處理過）" --> B
    C -- "否" --> D

    D["執行 CMD\n./export --LOT_ID xx --WAFER_ID xx\n--LAYER_ID xx --output_dir xx"]
    D --> E{CMD 成功?}
    E -- "失敗" --> F{已重試 3 次?}
    F -- "否" --> D
    F -- "是" --> G([❌ KLARF Failed\nklarfFail++])

    E -- "成功" --> H[讀取 KLARF 檔案末尾\n128 bytes]
    H --> I{最後非空行\n== 'EndOfFile;' ?}
    I -- "不符" --> F
    I -- "符合" --> J([✅ KLARF validated\nklarfSuccess++])

    J --> K[開始 Poll Target\n每 1 分鐘查詢一次]
    K --> L{Target 有此筆資料?}
    L -- "是" --> M([✅ Target Confirmed\ntargetSuccess++])
    L -- "否" --> N{已查詢 10 次?}
    N -- "否" --> O[等待 1 分鐘] --> K
    N -- "是" --> P([⏰ Target Timeout\ntargetTimeout++])

    B --> Q([所有 LAYER 完成])
```



---

## Worker 路由機制

```mermaid
flowchart LR
    subgraph CYCLE["Cycle（每 5 分鐘）"]
        Q1["LOT001/WAFER01\n3 layers"]
        Q2["LOT001/WAFER02\n2 layers"]
        Q3["LOT002/WAFER01\n3 layers"]
        Q4["LOT002/WAFER03\n2 layers"]
        Q5["LOT003/WAFER01\n3 layers"]
        Q6["LOT004/WAFER02\n2 layers"]
        Q7["LOT005/WAFER01\n2 layers"]
    end

    subgraph HASH["hash(LOT:WAFER) % 6"]
        H[FNV-32a Hash]
    end

    subgraph POOL["Worker Pool"]
        W0[Worker 0\nbuffered ch:50]
        W1[Worker 1\nbuffered ch:50]
        W2[Worker 2\nbuffered ch:50]
        W3[Worker 3\nbuffered ch:50]
        W4[Worker 4\nbuffered ch:50]
        W5[Worker 5\nbuffered ch:50]
    end

    Q1 & Q2 & Q3 & Q4 & Q5 & Q6 & Q7 --> H
    H --> W0 & W1 & W2 & W3 & W4 & W5
```



> **保證**：相同 `LOT_ID + WAFER_ID` 的 Job 永遠由同一個 Worker 串行執行，避免同組資料並行衝突。

---

## 設定檔說明 (`config/config.yaml`)

| 欄位                            | 預設值                | 說明                                         |
| ----------------------------- | ------------------ | ------------------------------------------ |
| `database.driver`             | `mysql`            | 資料庫驅動：`mysql` 或 `oracle`                   |
| `database.host`               | `127.0.0.1`        | 資料庫主機位址                                    |
| `database.port`               | `3306`             | 資料庫連接埠（Oracle 預設 `1521`）                   |
| `database.user`               | `klarf_user`       | 資料庫帳號                                      |
| `database.password`           | `klarf_pass`       | 資料庫密碼                                      |
| `database.dbname`             | `klarf_db`         | MySQL: database 名稱 / Oracle: service name  |
| `worker.count`                | `6`                | Worker 數量                                  |
| `export.command`              | `./export`         | export 可執行檔路徑（Windows 加 `.exe`）            |
| `export.output_dir`           | `./klarf_output`   | KLARF 文件輸出目錄                               |
| `polling.source_interval`     | `5m`               | Source table 掃描間隔                          |
| `polling.target_interval`     | `1m`               | Target table 輪詢間隔                          |
| `polling.target_max_attempts` | `10`               | Target 最多輪詢次數（= 10 分鐘 timeout）             |
| `retry.max_attempts`          | `3`                | CMD 失敗最多重試次數                               |
| `log.level`                   | `info`             | `debug` / `info` / `warn` / `error`        |
| `log.file`                    | `./logs/klarf.log` | log 檔案路徑（留空則只輸出 stdout）                    |

---

## ⚙️ 可設定項目

所有可調整的設定都集中在 **`config/config.yaml`** 這一個檔案中，無需修改任何 Go 原始碼。

---

### 1. 切換資料庫驅動（MySQL ↔ Oracle）

修改 `config/config.yaml` 中的 `database` 區塊：

**MySQL（預設）**
```yaml
database:
  driver:   "mysql"
  host:     "127.0.0.1"
  port:     3306
  user:     "klarf_user"
  password: "klarf_pass"
  dbname:   "klarf_db"        # MySQL database 名稱
```

**Oracle**
```yaml
database:
  driver:   "oracle"
  host:     "your-oracle-host"
  port:     1521
  user:     "klarf_user"
  password: "klarf_pass"
  dbname:   "ORCL"            # Oracle service name
```

> **說明**
> - `driver: "mysql"` → 使用 `github.com/go-sql-driver/mysql`，DSN 格式：`user:pass@tcp(host:port)/dbname?parseTime=true&charset=utf8mb4`
> - `driver: "oracle"` → 使用 `github.com/sijms/go-ora/v2`，DSN 格式：`oracle://user:pass@host:port/service_name`
> - SQL 語法差異（日期、placeholder）由 `db/db.go` 內部自動切換，使用者無需操心。

---

### 2. KLARF 文件輸出位置

修改 `config/config.yaml` 中的 `export.output_dir`：

```yaml
export:
  command:    "./export.exe"          # Windows: ./export.exe | Linux/macOS: ./export
  output_dir: "./klarf_output"        # ← 修改這裡
```

| 範例值                         | 說明                       |
| ----------------------------- | -------------------------- |
| `"./klarf_output"`            | 程式執行目錄下的相對路徑（預設）  |
| `"D:/data/klarf"`             | Windows 絕對路徑              |
| `"/mnt/nas/klarf"`            | Linux / NAS 絕對路徑          |
| `"/opt/klarf/output"`         | Linux 系統路徑                |

> 目錄不存在時程式啟動後會**自動建立**（`os.MkdirAll`），無需手動新增。

KLARF 檔案命名規則：`{output_dir}/{LOT_ID}_{WAFER_ID}_{LAYER_ID}.klarf`

---

### 3. Log 檔案位置

修改 `config/config.yaml` 中的 `log` 區塊：

```yaml
log:
  level: "info"                       # debug / info / warn / error
  file:  "./logs/klarf.log"           # ← 修改這裡（留空 "" 則只輸出到 stdout）
```

| 範例值                          | 說明                            |
| ------------------------------ | ------------------------------- |
| `"./logs/klarf.log"`           | 程式執行目錄下的相對路徑（預設）     |
| `"D:/logs/klarf/processor.log"`| Windows 絕對路徑                 |
| `"/var/log/klarf/processor.log"`| Linux 系統 log 目錄              |
| `""`                           | 停用檔案 log，僅輸出到 stdout      |

> Log 目錄不存在時會**自動建立**。所有 log 同時輸出到 **stdout** 及指定檔案（`io.MultiWriter`）。

---

### 完整 `config/config.yaml` 範例

```yaml
database:
  driver:   "mysql"          # "mysql" 或 "oracle"
  host:     "127.0.0.1"
  port:     3306
  user:     "klarf_user"
  password: "klarf_pass"
  dbname:   "klarf_db"

worker:
  count: 6                   # Worker 數量

export:
  command:    "./export.exe" # Linux/macOS: "./export"
  output_dir: "./klarf_output"

polling:
  source_interval:     "5m"  # Source 掃描間隔
  target_interval:     "1m"  # Target 輪詢間隔
  target_max_attempts: 10    # Target 最多輪詢次數

retry:
  max_attempts: 3            # CMD 失敗最多重試次數

log:
  level: "info"              # debug / info / warn / error
  file:  "./logs/klarf.log"  # 留空 "" 則只輸出到 stdout
```

---

## 執行方式

### 1. 啟動 MySQL（Docker）

```bash
docker-compose up -d
```

### 2. 編譯

```bash
# Linux / macOS
go build -o export ./mock_export/
go build -o klarf-processor .

# Windows
go build -o export.exe ./mock_export/
go build -o klarf-processor.exe .
```

### 3. 執行

```bash
# Linux / macOS
./klarf-processor

# Windows
./klarf-processor.exe
```

### 4. 停止（優雅關閉）

```
Ctrl+C   →  等待 Worker 完成當前任務  →  印出統計摘要  →  結束
```

---

## Log 格式

所有 log 以 **JSON** 輸出，同時寫入 `stdout` 及 `logs/klarf.log`：

```json
{"time":"2026-03-03T23:35:03+08:00","level":"info","msg":"KLARF validated OK","lot_id":"LOT003","wafer_id":"WAFER01","layer_id":"M1","attempt":1,"path":"klarf_output/LOT003_WAFER01_M1.klarf"}
{"time":"2026-03-03T23:35:03+08:00","level":"info","msg":"start polling target","lot_id":"LOT003","wafer_id":"WAFER01","layer_id":"M1","interval":60000000000,"max_attempts":10}
{"time":"2026-03-03T23:35:03+08:00","level":"info","msg":"target confirmed ✓","lot_id":"LOT003","wafer_id":"WAFER01","layer_id":"M1","attempt":2}
```

---

## 統計摘要（Ctrl+C 後輸出）

```
╔═══════════════════════════════════════════╗
║         KLARF Processor  Summary          ║
╠═══════════════════════════════════════════╣
║  Total Cycles       : 3                   ║
║  KLARF Generated    : 17                  ║
║  KLARF Failed       : 0                   ║
║  Target Confirmed   : 17                  ║
║  Target Timeout     : 0                   ║
║  CMD Retries        : 0                   ║
╚═══════════════════════════════════════════╝
```

---

## 資料庫 Schema

```sql
-- 待處理資料（來源）
CREATE TABLE source (
    id       INT AUTO_INCREMENT PRIMARY KEY,
    LOT_ID   VARCHAR(64) NOT NULL,
    WAFER_ID VARCHAR(64) NOT NULL,
    LAYER_ID VARCHAR(64) NOT NULL,
    scandate DATETIME    NOT NULL,
    INDEX idx_scandate  (scandate),
    INDEX idx_lot_wafer (LOT_ID, WAFER_ID)
);

-- 處理結果（目標）
CREATE TABLE target (
    id         INT AUTO_INCREMENT PRIMARY KEY,
    LOT_ID     VARCHAR(64) NOT NULL,
    WAFER_ID   VARCHAR(64) NOT NULL,
    LAYER_ID   VARCHAR(64) NOT NULL,
    scandate   DATETIME,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_lot_wafer_layer (LOT_ID, WAFER_ID, LAYER_ID)
);
```

