SOURCES := $(shell find . -type f -name "*.go" | grep -v /vendor)

zeno: $(SOURCES)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno && upx zeno

zeno-cli: $(SOURCES)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno-cli && upx zeno-cli

zenotool: $(SOURCES)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zenotool && upx zenotool
