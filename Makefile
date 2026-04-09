LITESTREAM_SHA ?= main
WORKER_IMAGE ?= registry.fly.io/litestream-soak:worker-$(shell git rev-parse --short HEAD 2>/dev/null || echo dev)

.PHONY: build build-worker test vet clean

build:
	go build -o bin/soakworker ./cmd/soakworker
	go build -o bin/soakctl ./cmd/soakctl

build-worker:
	go build -o bin/soakworker ./cmd/soakworker

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

docker-worker:
	docker build -f Dockerfile.worker --build-arg LITESTREAM_SHA=$(LITESTREAM_SHA) -t $(WORKER_IMAGE) .
