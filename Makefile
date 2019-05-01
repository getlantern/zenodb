VENDOR_SOURCE := Gopkg.lock

SOURCES := $(shell find . -type f -name "*.go" | grep -v /vendor)

zeno: vendor $(SOURCES)
	GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno && upx zeno

zeno-cli: vendor $(SOURCES)
	GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno-cli && upx zeno-cli

zenotool: vendor $(SOURCES)
	GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zenotool && upx zenotool

vendor: $(VENDOR_SOURCE)
	dep ensure
