// mock_export 模擬真實的 export 指令，根據 LOT/WAFER/LAYER 產生符合規格的 KLARF 1.1 文件。
// 使用方式：./export --LOT_ID <id> --WAFER_ID <id> --LAYER_ID <id> [--output_dir <dir>]
package main

import (
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	lotID   := flag.String("LOT_ID",     "",               "Lot ID (required)")
	waferID := flag.String("WAFER_ID",   "",               "Wafer ID (required)")
	layerID := flag.String("LAYER_ID",   "",               "Layer ID (required)")
	outDir  := flag.String("output_dir", "./klarf_output", "KLARF output directory")
	flag.Parse()

	if *lotID == "" || *waferID == "" || *layerID == "" {
		fmt.Fprintln(os.Stderr, "usage: export --LOT_ID <id> --WAFER_ID <id> --LAYER_ID <id> [--output_dir <dir>]")
		os.Exit(1)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 模擬處理時間 (300ms ~ 1500ms)
	delay := time.Duration(300+rng.Intn(1200)) * time.Millisecond
	fmt.Printf("[mock-export] LOT=%-8s WAFER=%-8s LAYER=%-10s  processing (%.1fs)...\n",
		*lotID, *waferID, *layerID, delay.Seconds())
	time.Sleep(delay)

	// 產生 KLARF 內容
	content := buildKlarf(*lotID, *waferID, *layerID, rng)

	// 確保輸出目錄存在
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[mock-export] mkdir %s: %v\n", *outDir, err)
		os.Exit(1)
	}

	// 寫入 KLARF 文件
	fname := fmt.Sprintf("%s_%s_%s.klarf", *lotID, *waferID, *layerID)
	path  := filepath.Join(*outDir, fname)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "[mock-export] write %s: %v\n", path, err)
		os.Exit(1)
	}

	fmt.Printf("[mock-export] KLARF written → %s\n", path)
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

		// 最後一筆結尾加分號（KLARF 格式規定）
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
// e.g. "WAFER01" → 1, "W25" → 25, "WAFER" → 1
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
