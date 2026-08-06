# ppp build helpers.

.PHONY: generate build test vet

# generate regenerates Go code from the proto definitions (requires buf).
generate:
	buf generate

# build compiles all packages.
build:
	go build ./...

# test runs the full test suite.
test:
	go test ./...

# vet runs static analysis.
vet:
	go vet ./...
