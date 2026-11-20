.PHONY: build test bench vet
build:
	go build -o bin/pgfleet ./cmd/pgfleet
test:
	go vet ./... && go test -race -count=1 ./...
bench: build
	./bin/pgfleet bench -nodes 32 -iterations 20 -workers 8
