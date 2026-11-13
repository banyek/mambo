.PHONY: all deps build linux darwin clean

all: build

deps:
	go mod download

build:
	go build -o mambo .

linux:
	GOOS=linux go build -o mambo .

darwin:
	GOOS=darwin go build -o mambo .

clean:
	rm -f mambo
