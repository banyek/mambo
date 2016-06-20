all: build
deps: 
	GOPATH=$(shell pwd) go get github.com/cactus/go-statsd-client/statsd
	GOPATH=$(shell pwd) go get github.com/go-sql-driver/mysql
	GOPATH=$(shell pwd) go get github.com/koding/logging
	GOPATH=$(shell pwd) go get gopkg.in/ini.v1
build: deps
	GOPATH=$(shell pwd) go build -v -x mambo.go
linux: deps
	GOPATH=$(shell pwd) GOOS=linux go build -v -x mambo.go
darwin: deps
	GOPATH=$(shell pwd) GOOS=darwin go build -v -x mambo.go
clean:
	rm -rf src/
	rm -rf pkg/
	rm -rf mambo
	rm -rf mambo.cfg
