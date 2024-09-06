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

opt-fmt:
	opa fmt --write ./classification/labels

opa-lint:
	regal lint ./classification/labels/

opa-test:
	opa test ./classification/labels/*.rego -v

docker-build:
	docker build --build-arg VERSION="$(git describe --tags --always)" -t dmap .
