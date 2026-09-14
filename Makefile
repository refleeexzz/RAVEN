# RAVEN — Distributed Systems Platform
# One Makefile to rule the whole platform. Requires Go 1.27+, Docker.

GO            ?= go
SERVICES      := gateway auth users jobs websocket broker worker
BIN           := bin
PROTOC        ?= ./tools/protoc/bin/protoc
PROTO_OUT     := internal/gen
COVERAGE_OUT  := coverage.out

.DEFAULT_GOAL := help

## help: show this help
.PHONY: help
help:
	@echo "RAVEN platform — available targets:"
	@echo "  build         build all service binaries into $(BIN)/"
	@echo "  test          run unit tests"
	@echo "  test-race     run tests with the race detector"
	@echo "  coverage      run tests and write $(COVERAGE_OUT)"
	@echo "  lint          run go vet (golangci-lint if installed)"
	@echo "  vuln          run govulncheck"
	@echo "  proto         regenerate gRPC code from proto/"
	@echo "  migrate       apply database migrations"
	@echo "  docker-build  build all service images"
	@echo "  docker-up     start the full stack with Docker Compose"
	@echo "  docker-down   stop the stack"
	@echo "  k8s-up        deploy to the local Kubernetes cluster"
	@echo "  k8s-down      remove the Kubernetes deployment"
	@echo "  logs          tail Docker Compose logs"
	@echo "  benchmark     run benchmarks with -benchmem"
	@echo "  fmt           gofmt + goimports over the repo"

## build: compile every service
.PHONY: build
build: $(SERVICES)

.PHONY: $(SERVICES)
$(SERVICES):
	$(GO) build -o $(BIN)/$@ ./cmd/$@

## test: unit tests
.PHONY: test
test:
	$(GO) test ./...

## test-race: race detector
.PHONY: test-race
test-race:
	$(GO) test -race ./...

## test-integration: integration tests (needs Docker)
## WORKER_WEBHOOK_ALLOW_PRIVATE lets the SSRF egress guard reach the
## loopback httptest servers used by the webhook suites.
.PHONY: test-integration
test-integration:
	WORKER_WEBHOOK_ALLOW_PRIVATE=true $(GO) test -tags=integration ./tests/integration/... ./tests/security/...

## coverage
.PHONY: coverage
coverage:
	$(GO) test -coverprofile=$(COVERAGE_OUT) ./...
	$(GO) tool cover -func=$(COVERAGE_OUT)

## lint
.PHONY: lint
lint:
	$(GO) vet ./...

## vuln: vulnerability scan
.PHONY: vuln
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest ./...

## fmt
.PHONY: fmt
fmt:
	gofmt -w .

## proto: regenerate gRPC stubs
.PHONY: proto
proto:
	$(PROTOC) --proto_path=proto \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		proto/common/common.proto proto/auth/auth.proto proto/users/users.proto proto/jobs/jobs.proto

## migrate: apply SQL migrations
.PHONY: migrate
migrate:
	$(GO) run ./cmd/migrate up

## docker-build
.PHONY: docker-build
docker-build:
	docker compose build

## docker-up
.PHONY: docker-up
docker-up:
	docker compose up -d

## docker-down
.PHONY: docker-down
docker-down:
	docker compose down

## logs
.PHONY: logs
logs:
	docker compose logs -f

## k8s-up: apply all manifests
.PHONY: k8s-up
k8s-up:
	kubectl apply -f deployments/kubernetes/

## k8s-down
.PHONY: k8s-down
k8s-down:
	kubectl delete -f deployments/kubernetes/ --ignore-not-found

## benchmark
.PHONY: benchmark
benchmark:
	$(GO) test -run=^$$ -bench=. -benchmem ./...
