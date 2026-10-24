.PHONY: help setup test lint demo-skew demo-compaction benchmark generate-data run-bronze run-silver run-gold run-all run-streaming docker-up docker-down clean

help:
	@echo "Available commands:"
	@echo "  make setup            Install dependencies"
	@echo "  make test             Run pytest test suite"
	@echo "  make demo-skew        Run interactive data skew fix demo"
	@echo "  make demo-compaction  Run interactive small files compaction demo"
	@echo "  make benchmark        Execute automated benchmark suite and generate artifacts"
	@echo "  make generate-data    Generate 100K skewed sample records"
	@echo "  make run-bronze       Execute Bronze ingestion layer"
	@echo "  make run-silver       Execute Silver cleansing & enrichment layer"
	@echo "  make run-gold         Execute Gold business aggregation layer"
	@echo "  make run-all          Run full Medallion pipeline end-to-end (batch)"
	@echo "  make run-streaming    Run continuous streaming pipeline (Bronze -> Silver -> Gold)"
	@echo "  make docker-up        Spin up Kafka + Zookeeper + Spark cluster"
	@echo "  make docker-down      Tear down Docker containers"
	@echo "  make clean            Remove build artifacts and temp data"

setup:
	pip install -r requirements.txt

test:
	pytest tests/

demo-skew:
	python scripts/demo_skew_fix.py

demo-compaction:
	python scripts/demo_compaction.py

benchmark:
	python scripts/run_benchmarks.py

generate-data:
	python scripts/generate_sample_data.py --orders 100000 --clickstream 200000

run-bronze:
	python scripts/run_pipeline.py --layer bronze --mode batch

run-silver:
	python scripts/run_pipeline.py --layer silver --mode batch

run-gold:
	python scripts/run_pipeline.py --layer gold --mode batch

run-all:
	python scripts/run_pipeline.py --layer all --mode batch

run-streaming:
	python scripts/run_pipeline.py --layer all --mode streaming

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	rm -rf metastore_db derby.log spark-warehouse data/demo_compaction .pytest_cache
	find . -type d -name "__pycache__" -exec rm -rf {} +
