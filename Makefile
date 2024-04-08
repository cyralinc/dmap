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
	regal lint --disable=line-length ./classification/labels/

opa-test:
	opa test ./classification/labels/*.rego -v
