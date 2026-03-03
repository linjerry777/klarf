-- ============================================================
-- KLARF Processor - Database Schema
-- ============================================================

CREATE TABLE IF NOT EXISTS `source` (
    `id`       INT AUTO_INCREMENT PRIMARY KEY,
    `LOT_ID`   VARCHAR(64) NOT NULL,
    `WAFER_ID` VARCHAR(64) NOT NULL,
    `LAYER_ID` VARCHAR(64) NOT NULL,
    `scandate` DATETIME    NOT NULL,
    INDEX `idx_scandate`  (`scandate`),
    INDEX `idx_lot_wafer` (`LOT_ID`, `WAFER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `target` (
    `id`         INT AUTO_INCREMENT PRIMARY KEY,
    `LOT_ID`     VARCHAR(64) NOT NULL,
    `WAFER_ID`   VARCHAR(64) NOT NULL,
    `LAYER_ID`   VARCHAR(64) NOT NULL,
    `scandate`   DATETIME,
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_lot_wafer_layer` (`LOT_ID`, `WAFER_ID`, `LAYER_ID`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- Test Data：20 筆
-- Worker 路由分組（hash by LOT+WAFER）：
--   Group A: LOT001/WAFER01 → 3 layers (IMD1, IMD2, GATE)
--   Group B: LOT001/WAFER02 → 2 layers (M1, M2)
--   Group C: LOT002/WAFER01 → 3 layers (POLY, STI, NWELL)
--   Group D: LOT002/WAFER03 → 2 layers (VIA1, VIA2)
--   Group E: LOT003/WAFER01 → 3 layers (M1, M2, M3)
--   Group F: LOT004/WAFER02 → 2 layers (GATE, POLY)
--   Group G: LOT005/WAFER01 → 2 layers (STI, NWELL)
--   ─────────── 共 17 筆應被處理（scandate > 7 天）
--   Group H: LOT003/WAFER02, LOT004/WAFER01, LOT005/WAFER02
--   ─────────── 共 3 筆不應被處理（scandate ≤ 7 天）
-- ============================================================

INSERT INTO `source` (`LOT_ID`, `WAFER_ID`, `LAYER_ID`, `scandate`) VALUES

-- Group A: LOT001 / WAFER01 (3 layers, 10~15天前)
('LOT001', 'WAFER01', 'IMD1', DATE_SUB(NOW(), INTERVAL 10 DAY)),
('LOT001', 'WAFER01', 'IMD2', DATE_SUB(NOW(), INTERVAL 12 DAY)),
('LOT001', 'WAFER01', 'GATE', DATE_SUB(NOW(), INTERVAL 15 DAY)),

-- Group B: LOT001 / WAFER02 (2 layers, 8~9天前)
('LOT001', 'WAFER02', 'M1',   DATE_SUB(NOW(), INTERVAL 8  DAY)),
('LOT001', 'WAFER02', 'M2',   DATE_SUB(NOW(), INTERVAL 9  DAY)),

-- Group C: LOT002 / WAFER01 (3 layers, 14~20天前)
('LOT002', 'WAFER01', 'POLY',  DATE_SUB(NOW(), INTERVAL 20 DAY)),
('LOT002', 'WAFER01', 'STI',   DATE_SUB(NOW(), INTERVAL 18 DAY)),
('LOT002', 'WAFER01', 'NWELL', DATE_SUB(NOW(), INTERVAL 14 DAY)),

-- Group D: LOT002 / WAFER03 (2 layers, 11~13天前)
('LOT002', 'WAFER03', 'VIA1',  DATE_SUB(NOW(), INTERVAL 11 DAY)),
('LOT002', 'WAFER03', 'VIA2',  DATE_SUB(NOW(), INTERVAL 13 DAY)),

-- Group E: LOT003 / WAFER01 (3 layers, 16~25天前)
('LOT003', 'WAFER01', 'M1',    DATE_SUB(NOW(), INTERVAL 25 DAY)),
('LOT003', 'WAFER01', 'M2',    DATE_SUB(NOW(), INTERVAL 22 DAY)),
('LOT003', 'WAFER01', 'M3',    DATE_SUB(NOW(), INTERVAL 16 DAY)),

-- Group F: LOT004 / WAFER02 (2 layers, 28~30天前)
('LOT004', 'WAFER02', 'GATE',  DATE_SUB(NOW(), INTERVAL 30 DAY)),
('LOT004', 'WAFER02', 'POLY',  DATE_SUB(NOW(), INTERVAL 28 DAY)),

-- Group G: LOT005 / WAFER01 (2 layers, 13~17天前)
('LOT005', 'WAFER01', 'STI',   DATE_SUB(NOW(), INTERVAL 13 DAY)),
('LOT005', 'WAFER01', 'NWELL', DATE_SUB(NOW(), INTERVAL 17 DAY)),

-- ── 以下 3 筆 scandate ≤ 7 天，不會被查詢到 ──────────────────
('LOT003', 'WAFER02', 'M1',    DATE_SUB(NOW(), INTERVAL 2  DAY)),
('LOT004', 'WAFER01', 'STI',   DATE_SUB(NOW(), INTERVAL 5  DAY)),
('LOT005', 'WAFER02', 'GATE',  DATE_SUB(NOW(), INTERVAL 3  DAY));
