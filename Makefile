# ============================================
# Akhdar BI Command Center - Makefile
# ============================================

.PHONY: up down load build test clean all help setup

# Default target
help:
	@echo "Akhdar BI Command Center - Available Commands"
	@echo "=============================================="
	@echo ""
	@echo "  make setup    - Install Python dependencies"
	@echo "  make up       - Start Postgres container"
	@echo "  make down     - Stop Postgres container"
	@echo "  make load     - Load raw CSV data into Postgres"
	@echo "  make build    - Build staging tables and KPI marts"
	@echo "  make test     - Run data quality tests"
	@echo "  make clean    - Remove containers and volumes"
	@echo "  make all      - Run full pipeline (up + load + build + test)"
	@echo ""

# Install Python dependencies
setup:
	pip install -r requirements.txt

# Start Postgres
up:
	docker-compose up -d
	@echo "Waiting for Postgres to be ready..."
	@sleep 3
	@docker-compose exec -T postgres pg_isready -U akhdar -d akhdar_bi || (echo "Postgres not ready, waiting..." && sleep 5)
	@echo "Postgres is ready!"

# Stop Postgres
down:
	docker-compose down

# Load raw data
load:
	python -m etl.run_all --step load

# Build staging and marts
build:
	python -m etl.run_all --step build

# Run tests
test:
	pytest tests/ -v

# Clean everything
clean:
	docker-compose down -v
	rm -rf __pycache__ .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Full pipeline
all: up
	@sleep 2
	python -m etl.run_all
	pytest tests/ -v
	@echo ""
	@echo "✅ Pipeline complete! Data is ready for Power BI."
