LITESTREAM_SHA ?= main
LITESTREAM_REPO ?= ../../../benbjohnson/litestream
WORKER_IMAGE ?= registry.fly.io/litestream-soak:worker-$(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
LOCAL_DATA_DIR ?= /tmp/litestream-soak

.PHONY: build build-worker build-deps test vet clean run-local clean-local docker-worker compose-build refresh-worker-fleet

build:
	go build -o bin/soakworker ./cmd/soakworker
	go build -o bin/soakctl ./cmd/soakctl

build-worker:
	go build -o bin/soakworker ./cmd/soakworker

build-deps:
	cd $(LITESTREAM_REPO) && go build -o $(CURDIR)/bin/litestream ./cmd/litestream
	cd $(LITESTREAM_REPO) && go build -o $(CURDIR)/bin/litestream-test ./cmd/litestream-test

build-all: build build-deps

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

clean-local:
	rm -rf $(LOCAL_DATA_DIR)

run-local: build-worker
	@mkdir -p $(LOCAL_DATA_DIR)
	PATH="$(CURDIR)/bin:$(PATH)" \
	DATA_DIR="$(LOCAL_DATA_DIR)" \
	REPLICA_TYPE=file \
	REPLICA_PATH="$(LOCAL_DATA_DIR)/replicas" \
	PROFILE=low-volume \
	INITIAL_SIZE=1MB \
	VERIFY_INTERVAL=1m \
	SNAPSHOT_INTERVAL=2m \
	WORKER_ID=local-test \
	GIT_SHA=local \
	./bin/soakworker

test-replay: build-worker
	@rm -rf /tmp/litestream-soak-replay
	@mkdir -p /tmp/litestream-soak-replay
	PATH="$(CURDIR)/bin:$(PATH)" \
	DATA_DIR="/tmp/litestream-soak-replay" \
	REPLICA_TYPE=file \
	REPLICA_PATH="/tmp/litestream-soak-replay/replicas" \
	LOAD_MODE=replay \
	REPLAY_DATASET=taxi \
	REPLAY_DATA_PATH="$(CURDIR)/datasets/taxi_sample.csv" \
	REPLAY_SPEED=1000 \
	INITIAL_SIZE=1MB \
	VERIFY_INTERVAL=5m \
	SNAPSHOT_INTERVAL=2m \
	WORKER_ID=replay-test \
	GIT_SHA=local \
	./bin/soakworker

docker-worker:
	@litestream_sha="$$(./scripts/resolve-litestream-sha.sh "$(LITESTREAM_SHA)")" && \
	docker build -f Dockerfile.worker --build-arg LITESTREAM_SHA="$$litestream_sha" -t $(WORKER_IMAGE) .

compose-build:
	@litestream_sha="$$(./scripts/resolve-litestream-sha.sh "$(LITESTREAM_SHA)")" && \
	LITESTREAM_SHA="$$litestream_sha" docker compose build

refresh-worker-fleet:
	./scripts/refresh-worker-fleet.sh
