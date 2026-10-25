.PHONY: dev db-up db-down server web test test-core test-demo build generate generate-demo clean

dev: db-up
	@echo "Run 'make server' and 'make web' in separate terminals."

db-up:
	docker compose -p weave -f examples/demo/docker-compose.yml up -d --wait postgres

db-down:
	docker compose -p weave -f examples/demo/docker-compose.yml down

server:
	go -C examples/demo run ./cmd/server

web:
	npm run dev --workspace weave-demo-web

test: test-core test-demo

test-core:
	go test ./cmd/... ./internal/... ./pkg/...
	npm test

test-demo:
	go -C examples/demo test ./...
	npm run test:demo
	npm run build --workspace weave-demo-web

build:
	mkdir -p bin
	go -C examples/demo build -o ../../bin/demo-server ./cmd/server
	npm run build --workspace weave-demo-web

generate:
	buf generate

generate-demo:
	npm run generate:mutations
	npm run generate:projections

clean:
	rm -rf bin examples/demo/web/dist examples/demo/web/*.tsbuildinfo
