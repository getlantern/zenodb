SOURCES := $(shell find . -type f -name "*.go" | grep -v /vendor)

DEPS := go.mod go.sum

zeno: $(SOURCES) $(DEPS)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno && upx zeno

zeno-cli: $(SOURCES) $(DEPS)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zeno-cli && upx zeno-cli

zenotool: $(SOURCES) $(DEPS)
	GO111MODULE=on GOOS=linux GOARCH=amd64 go build github.com/getlantern/zenodb/cmd/zenotool && upx zenotool
