TARGET = _furrow
IMAGE = symfoni/furrow
GITCOMMIT = `git rev-parse --short HEAD`
DATE = `date`

.PHONEY: clean test

clean:
	rm $(TARGET) 
	go clean

test:
	go vet


build: test
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags "-X 'github.com/SymfoniNext/furrow_next/furrow_next.buildDate=$(DATE)' -X github.com/SymfoniNext/furrow_next_pl/furrow_next.commitID=$(GITCOMMIT) -w -extld ld -extldflags -static" -x -o $(TARGET) .

build-win: $ENV:CGO_ENABLED=0
  $ENV:GOOS="linux"
  go build -a -installsuffix cgo -ldflags "-X 'github.com/SymfoniNext/furrow_next_pl/furrow_next_pl.buildDate=$(DATE)' -X github.com/SymfoniNext/furrow_next_pl/furrow_next_pl.commitID=$(GITCOMMIT) -w -extld ld -extldflags -static" -x -o _furrow

docker: build
	docker build -t $(IMAGE):$(GITCOMMIT) .
