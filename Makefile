# Financial Reporting & Data Quality Engine
# make setup && make seed && make run   ->  five-minute local quickstart
SHELL := /bin/bash
PY ?= .venv/bin/python
FIN_DQ ?= .venv/bin/fin-dq
RUN_DATE ?= 2024-11-13
EVENTS ?= 200000
AWS_REGION ?= us-east-1
IMAGE_TAG ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo latest)
export FIN_DQ_TEST_DATABASE_URL ?= postgresql+psycopg://postgres:postgres@localhost:5432/fin_dq_test

.PHONY: setup db seed run backfill test test-unit test-integration lint typecheck check dashboard deploy destroy clean

setup: ## create the venv, install deps, install pre-commit, start Postgres
	uv venv --python 3.11 .venv
	uv pip install -e ".[dev,dashboard]"
	.venv/bin/pre-commit install
	$(MAKE) db

db: ## start Docker Postgres and create the test database
	docker compose up -d postgres
	@until docker compose exec -T postgres pg_isready -U postgres -d fin_dq >/dev/null 2>&1; do sleep 1; done
	docker compose exec -T postgres psql -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'fin_dq_test'" | grep -q 1 || \
		docker compose exec -T postgres psql -U postgres -c "CREATE DATABASE fin_dq_test"
	$(FIN_DQ) migrate --local

seed: ## generate synthetic data (~600k rows, 24 months) and load reference dimensions
	$(FIN_DQ) seed --local --events $(EVENTS)

run: ## run the six stages for RUN_DATE in local mode and print the Markdown summary
	$(FIN_DQ) run-daily --local --run-date $(RUN_DATE)

backfill: ## make backfill START=2025-01-01 END=2025-01-31
	$(FIN_DQ) run-daily --local --start $(START) --end $(END) --no-notify

test: ## full suite with coverage (needs Postgres: FIN_DQ_TEST_DATABASE_URL or Docker for testcontainers)
	$(PY) -m pytest --cov --cov-report=term-missing --cov-report=xml

test-unit: ## fast tests only
	$(PY) -m pytest tests/unit -q

test-integration: ## database-backed end-to-end tests
	$(PY) -m pytest tests/integration -q

lint:
	.venv/bin/ruff check src tests scripts infra
	.venv/bin/ruff format --check src tests scripts infra

typecheck:
	.venv/bin/mypy

check: lint typecheck test ## everything CI runs

dashboard: ## Streamlit KPI / scorecard / quarantine explorer
	.venv/bin/streamlit run dashboard/app.py

deploy: ## build + push the Lambda image, then cdk deploy --all
	@test -n "$(AWS_ACCOUNT_ID)" || (echo "set AWS_ACCOUNT_ID"; exit 1)
	aws ecr describe-repositories --repository-names fin-dq-pipeline --region $(AWS_REGION) >/dev/null 2>&1 || \
		aws ecr create-repository --repository-name fin-dq-pipeline --region $(AWS_REGION) >/dev/null
	aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com
	docker build -t fin-dq-pipeline:$(IMAGE_TAG) .
	docker tag fin-dq-pipeline:$(IMAGE_TAG) $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/fin-dq-pipeline:$(IMAGE_TAG)
	docker push $(AWS_ACCOUNT_ID).dkr.ecr.$(AWS_REGION).amazonaws.com/fin-dq-pipeline:$(IMAGE_TAG)
	cd infra && ../.venv/bin/python -m pip show aws-cdk-lib >/dev/null && npx --yes aws-cdk@2 deploy --all --require-approval never -c image_tag=$(IMAGE_TAG)

destroy:
	cd infra && npx --yes aws-cdk@2 destroy --all

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov coverage.xml infra/cdk.out
	find . -name "__pycache__" -type d -prune -exec rm -rf {} +
