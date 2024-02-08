all: tidy lint test

tidy:
	go mod tidy

lint:
	golangci-lint run

# Using --count=1 disables test caching
test:
	go test -v -race ./... --count=1

integration-test:
	go test -v -race ./... --count=1 --tags=integration

clean:
	go clean -i ./...
