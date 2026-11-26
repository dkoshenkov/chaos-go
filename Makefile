GO ?= go
DOCKER ?= docker
HELM ?= helm
IMAGE ?= chaos-proxy:dev
BIN_DIR ?= bin
VERSION ?= dev
COMMIT ?= unknown
BUILD_DATE ?= unknown

SERVER_PKG := github.com/dkoshenkov/chaos-go/internal/server
LDFLAGS := -X $(SERVER_PKG).version=$(VERSION) -X $(SERVER_PKG).commit=$(COMMIT) -X $(SERVER_PKG).buildDate=$(BUILD_DATE)
CHART := deploy/helm/chaos-proxy

.PHONY: build test vet docker-build helm-lint helm-template

build:
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/chaos-proxy ./cmd/chaos-proxy
	$(GO) build -trimpath -o $(BIN_DIR)/chaos-tunnel ./cmd/chaos-tunnel

test:
	$(GO) test -race ./...

vet:
	$(GO) vet ./...

docker-build:
	$(DOCKER) build --tag '$(IMAGE)' --build-arg 'VERSION=$(VERSION)' --build-arg 'COMMIT=$(COMMIT)' --build-arg 'BUILD_DATE=$(BUILD_DATE)' .

helm-lint:
	$(HELM) lint $(CHART)

helm-template:
	$(HELM) template chaos-proxy $(CHART)
