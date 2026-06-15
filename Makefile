

build:
	GOOS=linux GOARCH=amd64 go build -o ./bin/pg-upgrade ./cmd/pg-upgrade/main.go
	GOOS=linux GOARCH=amd64 go build -o ./bin/loadtool ./cmd/loadtool/main.go