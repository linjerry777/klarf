// mock_export 模擬真實的 export 指令，產生符合 KLARF 1.1 規格的文件。
//
// 檔名格式：{LOT_ID}.{seq:03d}
//   例：LOT_ID=PN0014.00 → PN0014.00.001, PN0014.00.002 ...
//   序號由當前 temp_dir 內已有的同 LOT 檔案數量決定（+1）。
//
// 使用方式：
//
//	./export --LOT_ID <id> --WAFER_ID <id> --LAYER_ID <id> --output_dir <dir>
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func main() {
	lotID   := flag.String("LOT_ID",     "",              "Lot ID (required)")
	waferID := flag.String("WAFER_ID",   "",              "Wafer ID (required)")
	layerID := flag.String("LAYER_ID",   "",              "Layer ID (required)")
	outDir  := flag.String("output_dir", "./klarf_temp",  "KLARF output directory")
	flag.Parse()

	if *lotID == "" || *waferID == "" || *layerID == "" {
		fmt.Fprintln(os.Stderr, "usage: export --LOT_ID <id> --WAFER_ID <id> --LAYER_ID <id> [--output_dir <dir>]")
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 模擬處理時間 300ms ~ 1500ms
	delay := time.Duration(300+rng.Intn(1200)) * time.Millisecond
	fmt.Printf("[mock-export] LOT=%-12s WAFER=%-8s LAYER=%-10s  processing (%.1fs)...\n",
		*lotID, *waferID, *layerID, delay.Seconds())
	time.Sleep(delay)

	// ── 模擬：約 15% 機率 CMD 成功但不產出任何檔案 ──────────────────────────
	// 對應真實情境：export 程式正常結束但因某些原因未寫入 KLARF
	if rng.Intn(100) < 15 {
		fmt.Printf("[mock-export] WARNING: simulated no-file-produced for LOT=%s WAFER=%s LAYER=%s\n",
			*lotID, *waferID, *layerID)
		os.Exit(0) // exit 0（CMD 成功），但沒有寫入任何檔案
	}

	// 確保輸出目錄存在
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[mock-export] mkdir %s: %v\n", *outDir, err)
		os.Exit(1)
	}

	// 決定下一個序號：掃描 outDir 內所有以 lotID 為前綴的檔案
	seq := nextSequence(*outDir, *lotID)
	fname := fmt.Sprintf("%s.%03d", *lotID, seq)
	path  := filepath.Join(*outDir, fname)

	// 產生 KLARF 內容
	content := buildKlarf(*lotID, *waferID, *layerID, rng)

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[mock-export] write %s: %v\n", path, err)
		os.Exit(1)
	}

	fmt.Printf("[mock-export] KLARF written → %s  (seq=%03d)\n", path, seq)
}

// nextSequence 掃描 dir 內所有以 lotID 為前綴、且後綴為 .NNN（數字）的檔案，
// 回傳目前最大序號 +1（從 1 開始）。
func nextSequence(dir, lotID string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 1
	}

	max := 0
	prefix := lotID + "."
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		suffix := e.Name()[len(prefix):]
		if n, err := strconv.Atoi(suffix); err == nil && n > max {
			max = n
		}
	}
	return max + 1
}

// ─── KLARF Builder ────────────────────────────────────────────────────────────

// buildKlarf 依照 KLARF 1.1 規格產生完整文件內容。
func buildKlarf(lotID, waferID, layerID string, rng *rand.Rand) string {
	now        := time.Now()
	ts         := now.Format("01-02-06 15:04:05")
	slotNum    := slotFromWaferID(waferID)
	numDefects := 3 + rng.Intn(18) // 隨機產生 3~20 個缺陷

	var b strings.Builder

	// ── Header ────────────────────────────────────────────────────────────────
	fmt.Fprintf(&b, "FileVersion 1 1;\n")
	fmt.Fprintf(&b, "FileTimestamp %s;\n", ts)
	fmt.Fprintf(&b, "InspectionStationID \"AMAT\" \"COMPLUS 3T\" \"HYCPS04\";\n")
	fmt.Fprintf(&b, "SampleType WAFER;\n")
	fmt.Fprintf(&b, "ResultTimestamp %s;\n", ts)
	fmt.Fprintf(&b, "LotID \"%s\";\n", lotID)
	fmt.Fprintf(&b, "SampleSize 1 200;\n")
	fmt.Fprintf(&b, "SetupID \"%s\" %s;\n", layerID, ts)
	fmt.Fprintf(&b, "StepID \"%s\";\n", layerID)
	fmt.Fprintf(&b, "SampleOrientationMarkType NOTCH;\n")
	fmt.Fprintf(&b, "OrientationMarkLocation DOWN;\n")
	fmt.Fprintf(&b, "DiePitch 2.4899600000e+03 2.2599200000e+03;\n")
	fmt.Fprintf(&b, "DieOrigin 0.000000 0.000000;\n")

	// ── Wafer Info ────────────────────────────────────────────────────────────
	fmt.Fprintf(&b, "WaferID \"%s\";\n", waferID)
	fmt.Fprintf(&b, "Slot %d;\n", slotNum)
	fmt.Fprintf(&b, "SampleCenterLocation 2.3948000000e+03 2.0903200000e+03;\n")

	// ── Class Lookup ─────────────────────────────────────────────────────────
	fmt.Fprintf(&b, "ClassLookup 5\n")
	fmt.Fprintf(&b, " 0 \"Undefined\"\n")
	fmt.Fprintf(&b, " 1 \"Pattern\"\n")
	fmt.Fprintf(&b, " 2 \"Particle\"\n")
	fmt.Fprintf(&b, " 3 \"Scratch\"\n")
	fmt.Fprintf(&b, " 4 \"Unknown\";\n")

	// ── Defect List ───────────────────────────────────────────────────────────
	fmt.Fprintf(&b, "AreaPerTest 2.3296152996e+10;\n")
	fmt.Fprintf(&b, "DefectRecordSpec 14 DEFECTID XREL YREL XINDEX YINDEX XSIZE YSIZE DEFECTAREA DSIZE CLASSNUMBER TEST ROUGHBINNUMBER IMAGECOUNT IMAGELIST;\n")
	fmt.Fprintf(&b, "DefectList\n")

	const totalDie = 4988
	dieSet := make(map[[2]int]bool)

	for i := 1; i <= numDefects; i++ {
		xrel  := rng.Float64() * 4000.0
		yrel  := rng.Float64() * 4000.0
		xi    := rng.Intn(61) - 30
		yi    := rng.Intn(61) - 30
		xsz   := 1.360 + rng.Float64()*9.0
		ysz   := 1.840 + rng.Float64()*9.0
		area  := xsz * ysz
		dsize := math.Sqrt(area)
		cls   := rng.Intn(5)

		end := ""
		if i == numDefects {
			end = ";"
		}
		fmt.Fprintf(&b, " %d %.10e %.10e %d %d %.6f %.6f %.6f %.10e %d 1 0 0 0%s\n",
			i, xrel, yrel, xi, yi, xsz, ysz, area, dsize, cls, end)

		dieSet[[2]int{xi, yi}] = true
	}

	// ── Summary ───────────────────────────────────────────────────────────────
	density := float64(numDefects) / float64(totalDie)
	nDefDie := len(dieSet)

	fmt.Fprintf(&b, "SummarySpec 5 TESTNO NDEFECT DEFDENSITY NDIE NDEFDIE ;\n")
	fmt.Fprintf(&b, "SummaryList\n")
	fmt.Fprintf(&b, " 1 %d %.6f %d %d;\n", numDefects, density, totalDie, nDefDie)

	// ── Footer ────────────────────────────────────────────────────────────────
	fmt.Fprintf(&b, "WaferStatus SSSSSSSSSSSSSSSSSSSSSSSSP ;\n")
	fmt.Fprintf(&b, "EndOfFile;\n")

	return b.String()
}

// slotFromWaferID 從 WaferID 字串尾端萃取數字作為 Slot 編號。
func slotFromWaferID(id string) int {
	n := 0
	for _, c := range id {
		if c >= '0' && c <= '9' {
			n = n*10 + int(c-'0')
		}
	}
	if n == 0 {
		return 1
	}
	return n
}
