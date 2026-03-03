.PHONY: build run deps docker-up docker-down docker-logs db-check clean

# ── Build & Run ────────────────────────────────────────────────────────────────
build:
	go build -o klarf-processor .

run: build
	./klarf-processor

# ── Dependencies ──────────────────────────────────────────────────────────────
deps:
	go mod tidy

# ── Docker ────────────────────────────────────────────────────────────────────
docker-up:
	docker-compose up -d
	@echo "Waiting for MySQL to be healthy..."
	@until docker inspect --format='{{.State.Health.Status}}' klarf_mysql 2>/dev/null | grep -q "healthy"; do \
		sleep 2; \
		printf "."; \
	done
	@echo ""
	@echo "MySQL is ready!"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f mysql

# ── DB Check ──────────────────────────────────────────────────────────────────
db-check:
	@echo "=== source table ==="
	docker exec klarf_mysql mysql -uklarf_user -pklarf_pass klarf_db \
		-e "SELECT LOT_ID, WAFER_ID, LAYER_ID, scandate, DATEDIFF(NOW(), scandate) AS days_ago FROM source ORDER BY LOT_ID, WAFER_ID;"
	@echo ""
	@echo "=== target table ==="
	docker exec klarf_mysql mysql -uklarf_user -pklarf_pass klarf_db \
		-e "SELECT LOT_ID, WAFER_ID, LAYER_ID, created_at FROM target ORDER BY created_at DESC;"

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -f klarf-processor
	rm -rf klarf_output logs
