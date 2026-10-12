# Repo-level tooling (ADR 0014). Each service is an independent module: all Go
# commands run with GOWORK=off from the service directory.
SERVICES := producer consumer

GOLANGCI_LINT_VERSION := v2.14.0
GOVULNCHECK_VERSION   := v1.8.0
COVERAGE_MIN          := 80.0

BIN := $(CURDIR)/bin
GOLANGCI_LINT := $(BIN)/golangci-lint
export GOWORK := off

.PHONY: help build test test-integration coverage lint vuln boundaries up down smoke

help:
	@echo "targets: build test test-integration coverage lint vuln boundaries up down smoke"

build:
	@for s in $(SERVICES); do (cd services/$$s && go build ./...) || exit 1; done

# Unit tests plus the coverage gate on domain + application.
test:
	@for s in $(SERVICES); do \
		echo "== $$s"; \
		(cd services/$$s && go test -count=1 ./... && \
			go test -count=1 -coverprofile=coverage.out ./internal/domain/... ./internal/application/... && \
			total=$$(go tool cover -func=coverage.out | awk '/^total:/ {sub("%","",$$3); print $$3}') && \
			echo "domain+application coverage: $$total% (min $(COVERAGE_MIN)%)" && \
			awk -v t="$$total" -v m="$(COVERAGE_MIN)" 'BEGIN { exit (t+0 >= m+0) ? 0 : 1 }') || exit 1; \
	done

# Needs Docker (testcontainers).
test-integration:
	@for s in $(SERVICES); do (cd services/$$s && go test -count=1 -p 1 -tags=integration ./...) || exit 1; done

# Coverage report per module: coverage/<module>.html and .out. Unit tests only by
# default; `make coverage COVERAGE_ARGS=--integration` adds the Docker tests and
# merges both. A report, not a gate (the gate is in `make test`).
COVERAGE_ARGS ?=
coverage:
	@bash scripts/coverage.sh $(COVERAGE_ARGS)

$(GOLANGCI_LINT):
	@mkdir -p $(BIN)
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/$(GOLANGCI_LINT_VERSION)/install.sh | sh -s -- -b $(BIN) $(GOLANGCI_LINT_VERSION)

# Every Go module: both services and the load-test report tool. golangci-lint
# reads the repo-root .golangci.yml, which sets build-tags: [integration], so the
# Docker-backed test files are linted too; the vet runs cover both tag sets.
LINT_MODULES := services/producer services/consumer loadtest/report

lint: $(GOLANGCI_LINT)
	@for m in $(LINT_MODULES); do \
		echo "== $$m"; \
		test -z "$$(cd $$m && gofmt -l .)" || { echo "gofmt needed in $$m"; exit 1; }; \
		(cd $$m && go vet ./... && go vet -tags=integration ./... && $(GOLANGCI_LINT) run ./...) || exit 1; \
	done

vuln:
	@for s in $(SERVICES); do (cd services/$$s && go run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...) || exit 1; done

boundaries:
	@bash scripts/check-boundaries.sh

up:
	docker compose up -d --build --wait

down:
	docker compose down

# End-to-end smoke test in its own Compose project (ADR 0016).
smoke:
	bash loadtest/run.sh smoke
