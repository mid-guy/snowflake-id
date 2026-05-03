.PHONY: build test bench run tidy clean

BIN := bin/snowflake-server
PKG := ./cmd/snowflake-server

build:
	@mkdir -p bin
	go build -o $(BIN) $(PKG)

test:
	go test ./... -race -count=1

bench:
	go test ./snowflake -bench=. -benchmem -run=^$$

run: build
	./$(BIN)

tidy:
	go mod tidy

clean:
	rm -rf bin
