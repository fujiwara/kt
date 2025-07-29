export GO111MODULE:=on
export OS=$(shell uname | tr '[:upper:]' '[:lower:]')

build: GOOS ?= ${OS}
build: GOARCH ?= amd64
build:
	rm -f kt
	GOOS=${GOOS} GOARCH=${GOARCH} go build -ldflags "-X main.buildTime=`date --iso-8601=s` -X main.buildVersion=`git rev-parse HEAD | cut -c-7`" .

dist:
	goreleaser build --snapshot --clean

dep-up:
	docker compose -f ./test-dependencies.yml up -d

dep-down:
	docker compose -f ./test-dependencies.yml down

test: clean
	go test -v -vet=all -failfast -race

test-integration: clean test-secrets
	go test -v -vet=all -failfast -race -tags=integration

.PHONY: test-secrets test-integration
test-secrets:
	cd test-secrets ; /usr/bin/env bash create-certs.sh

clean:
	rm -f kt
	rm -rf dist/

run: build
	./kt
