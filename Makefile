.PHONY: fmt lint fix check

GOBIN := $(shell go env GOPATH)/bin
GOFUMPT ?= $(GOBIN)/gofumpt
GOIMPORTS ?= $(GOBIN)/goimports
GOLANGCI_LINT ?= $(GOBIN)/golangci-lint

fmt:
	$(GOFUMPT) -w .
	$(GOIMPORTS) -w .

lint:
	$(GOLANGCI_LINT) run

fix:
	$(GOLANGCI_LINT) run --fix

check: fmt lint
