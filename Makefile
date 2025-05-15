# If you need to generate a data file for testing,
# run `dd if=/dev/random of=/path/to/outfile bs=1M count=<filesize>`
# where <filesize> is the size of the file in megabytes.
# This will generate a file of random bytes.
FLAGS = -ldflags '-s -w -buildid=' -trimpath
BINS = $(notdir $(wildcard cmd/*))

all: generate bin build

debug: FLAGS += -race
debug: all

bin:
	mkdir bin

.PHONY: build
build: $(BINS)

.PHONY: generate
generate:
	protoc --proto_path=. --go_out=. --go_opt=module=dfs ./protos/*

.PHONY: $(BINS)
$(BINS):
	go build -o ./bin/$@ $(FLAGS) ./cmd/$@

.PHONY: clean
clean:
	rm -rf bin
