export GOPATH=$(pwd)
export GOBIN=$GOPATH/bin
 
go get github.com/cactus/go-statsd-client/statsd
go get github.com/go-sql-driver/mysql
go get github.com/koding/logging
go get gopkg.in/ini.v1


go install src/mambo.go
