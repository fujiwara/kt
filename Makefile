export GO111MODULE:=on
export OS=$(shell uname | tr '[:upper:]' '[:lower:]')

build: GOOS ?= ${OS}
build: GOARCH ?= amd64
build:
	rm -f kt
	GOOS=${GOOS} GOARCH=${GOARCH} go build -ldflags "-X main.buildTime=`date --iso-8601=s` -X main.buildVersion=`git rev-parse HEAD | cut -c-7`" .

dist:
	goreleaser build --snapshot --clean

dep-up: test-secrets
	docker compose -f ./test-dependencies.yml up -d

dep-down:
	docker compose -f ./test-dependencies.yml down

test: clean
	go test -v -vet=all -failfast -race

test-integration:
	go test -v -vet=all -failfast -race -tags=integration

.PHONY: test-secrets test-integration
test-secrets:
	cd test-secrets ; /usr/bin/env bash create-certs.sh

clean:
	rm -f kt
	rm -rf dist/
	rm -f test-secrets/*.crt test-secrets/*.key test-secrets/*.jks test-secrets/*.srl test-secrets/*_creds test-secrets/auth-ssl*.json test-secrets/kafka_server_jaas.conf test-secrets/client.properties

run: build
	./kt
