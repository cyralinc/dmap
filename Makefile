all: tidy test

tidy:
	go mod tidy

# Using --count=1 disables test caching
test:
	go test -v -race ./... --count=1

integration-test:
	go test -v -race ./... --count=1 --tags=integration

clean:
	go clean -i ./...
